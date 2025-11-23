use Mojolicious::Lite;
use DBI;
use MongoDB;
use JSON;
use File::Slurp;
use Data::Dumper;
use Mojo::JSON qw(decode_json);
use Mojo::UserAgent;


# Conectar a la base de datos SQLite
my $sqlite_path = 'data/almacen.sqlite';
my $dbh = DBI->connect("dbi:SQLite:dbname=$sqlite_path","","", { RaiseError => 1, AutoCommit => 1 })
  or die " No se pudo conectar a SQLite: $DBI::errstr";


# Conectar a MongoDB para cada colección en sus respectivos contenedores
my $mongo_personas  = MongoDB->connect('mongodb://personas_db:27017');
my $mongo_articulos = MongoDB->connect('mongodb://articulos_db:27017');
my $mongo_ventas    = MongoDB->connect('mongodb://ventas_db:27017');

my $personas_db  = $mongo_personas->get_database('personas_db');
my $articulos_db = $mongo_articulos->get_database('articulos_db');
my $ventas_db    = $mongo_ventas->get_database('ventas_db');

sub limpiar_valor_numerico {
    my ($valor) = @_;
    return undef unless defined $valor;
    $valor =~ s/[^\d.]//g;
    return $valor if $valor =~ /^\d+(\.\d+)?$/;
    return undef;
}


# Función para cargar datos desde archivos JSON a MongoDB
sub load_data_to_mongo {
    app->log->info("Esperando que los contenedores Mongo estén listos...");
    sleep(5);  # Espera 5 segundos para asegurar la conexión

    my @collections = (
        { name => 'personas',  db => $personas_db,  file => '/app/data/personas.json' },
        { name => 'articulos', db => $articulos_db, file => '/app/data/articulos.json' },
        { name => 'ventas',    db => $ventas_db,    file => '/app/data/ventas.json' },
    );

    for my $col (@collections) {
        if (-e $col->{file}) {
            app->log->info("Cargando datos de $col->{file}...");
            my $json_data = read_file($col->{file});
            my $data = decode_json($json_data);
            $col->{db}->get_collection($col->{name})->insert_many($data);
            app->log->info(" Datos cargados en colección: $col->{name}");
        } else {
            app->log->error(" No se encontró el archivo: $col->{file}");
        }
    }
}


# Función para registrar mensajes de depuración
sub log_debug {

}


# Función para extraer y cargar personas
sub etl_process_personas {
    app->log->info("Iniciando ETL para PERSONAS...");
    my $coll = $personas_db->get_collection('personas');
    my @data = $coll->find()->all;
    
    $dbh->do("BEGIN TRANSACTION");  

    foreach my $doc (@data) {
        delete $doc->{_id};

        # Validaciones mínimas
        next unless defined $doc->{numeroDocumento} && $doc->{numeroDocumento} =~ /^\d+$/;
        next unless defined $doc->{nombres} && $doc->{nombres} ne '';
        next unless defined $doc->{primerApellido} && $doc->{primerApellido} ne '';

        # Inserción
        $dbh->do(
            "INSERT OR IGNORE INTO personas
            (numeroDocumento, nombres, primerApellido, segundoApellido, fechaNacimiento, telefono, direccion, email)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            undef,
            $doc->{numeroDocumento},
            $doc->{nombres},
            $doc->{primerApellido},
            $doc->{segundoApellido} // undef,
            $doc->{fechaNacimiento} // undef,
            $doc->{telefono} // undef,
            $doc->{direccion} // undef,
            $doc->{email} // undef
        );
    }
    $dbh->do("COMMIT");  
    app->log->info("ETL PERSONAS completado."); 
}

# Función para extraer y cargar artículos
sub etl_process_articulos {
    app->log->info("Iniciando ETL para ARTÍCULOS...");
    my $coll = $articulos_db->get_collection('articulos');
    my @data = $coll->find()->all;

    $dbh->do("BEGIN TRANSACTION");  

    foreach my $doc (@data) {
        delete $doc->{_id};

        
        my $nombre   = $doc->{nombreArticulo};
        my $valor    = limpiar_valor_numerico($doc->{precioArticulo});
        my $cantidad = limpiar_valor_numerico($doc->{cantidadArticulo});

        next unless defined $nombre && $nombre ne '';
        next unless defined $valor;

        $dbh->do(
            "INSERT OR IGNORE INTO articulos (nombreArticulo, precioArticulo, cantidadArticulo)
             VALUES (?, ?, ?)",
            undef,
            $nombre, $valor, $cantidad // 0
        );
    }
    $dbh->do("COMMIT");  
    app->log->info("ETL ARTÍCULOS completado.");
}



# Función para extraer y cargar ventas
sub etl_process_ventas {
    app->log->info("Iniciando ETL para VENTAS...");
    my $coll = $ventas_db->get_collection('ventas');
    my @data = $coll->find()->all;
    
    $dbh->do("BEGIN TRANSACTION");  

    foreach my $doc (@data) {
        delete $doc->{_id};

        my $idComprador = limpiar_valor_numerico($doc->{idComprador});
        my $idArticulo  = limpiar_valor_numerico($doc->{idArticulo});
        my $cantidad    = limpiar_valor_numerico($doc->{cantidadProductos});
        my $precioTotal = limpiar_valor_numerico($doc->{precioTotal});

        next unless defined $idComprador && defined $idArticulo && defined $cantidad && defined $precioTotal;

        $dbh->do(
            "INSERT OR IGNORE INTO ventas (idComprador, idArticulo, cantidadProductos, precioTotal)
             VALUES (?, ?, ?, ?)",
            undef,
            $idComprador, $idArticulo, $cantidad, $precioTotal
        );
    }
    $dbh->do("COMMIT");  
    app->log->info("ETL VENTAS completado.");
}
# Endpoint POST 
post '/etl' => sub {
    my $c = shift;

    eval {
        etl_process_personas();
        etl_process_articulos();
        etl_process_ventas();
    };
    if ($@) {
        app->log->error("Error en ETL: $@");
        return $c->render(json => { status => 'error', message => "Error durante ETL: $@" });
    }

    $c->render(json => { status => 'ok', message => 'ETL completado → SQLite actualizado.' });
};



# Rutas para cargar datos
get '/load_data' => sub {
    my $c = shift;

    # Ejecutar la función que carga los datos desde los JSON a MongoDB
    load_data_to_mongo();

    $c->render(json => { status => 'ok', message => 'Datos cargados correctamente en MongoDB' });
};

# Rutas para obtener datos de MongoDB
get '/mongo/personas' => sub {
    my $c = shift;
    my $data = [$personas_db->get_collection('personas')->find()->all];
    $c->render(json => $data);
};

get '/mongo/articulos' => sub {
    my $c = shift;
    my $data = [$articulos_db->get_collection('articulos')->find()->all];
    $c->render(json => $data);
};

get '/mongo/ventas' => sub {
    my $c = shift;
    my $data = [$ventas_db->get_collection('ventas')->find()->all];
    $c->render(json => $data);
};

# Rutas para obtener datos de SQLite
get '/sqlite/personas' => sub {
    my $c = shift;
    my $sth = $dbh->prepare("SELECT * FROM personas ");
    $sth->execute();
    my $rows = $sth->fetchall_arrayref({});
    $c->render(json => $rows);
};

get '/sqlite/articulos' => sub {
    my $c = shift;
    my $sth = $dbh->prepare("SELECT * FROM articulos ");
    $sth->execute();
    my $rows = $sth->fetchall_arrayref({});
    $c->render(json => $rows);
};

get '/sqlite/ventas' => sub {
    my $c = shift;
    my $sth = $dbh->prepare("SELECT * FROM ventas ");
    $sth->execute();
    my $rows = $sth->fetchall_arrayref({});
    $c->render(json => $rows);
};

# Agregar una nueva persona
post '/sqlite/personas' => sub {
    my $c = shift;
    my $data = $c->req->json;
    $dbh->do("INSERT INTO personas (nombre, numeroDocumento, telefono) VALUES (?, ?, ?)",
        undef, $data->{nombre}, $data->{numeroDocumento}, $data->{telefono});
    $c->render(json => { status => 'ok', message => 'Persona agregada.' });
};

# Agregar un nuevo artículo
post '/sqlite/articulos' => sub {
    my $c = shift;
    my $data = $c->req->json;
    $dbh->do("INSERT INTO articulos (nombreArticulo, valorArticulo) VALUES (?, ?)",
        undef, $data->{nombreArticulo}, $data->{valorArticulo});
    $c->render(json => { status => 'ok', message => 'Artículo agregado.' });
};

# Agregar una nueva venta
post '/sqlite/ventas' => sub {
    my $c = shift;
    my $data = $c->req->json;
    $dbh->do("INSERT INTO ventas (idComprador, idArticulo, cantidadProductos) VALUES (?, ?, ?)",
        undef, $data->{idComprador}, $data->{idArticulo}, $data->{cantidadProductos});
    $c->render(json => { status => 'ok', message => 'Venta agregada.' });
};

# Ruta raíz para probar que el servidor responde
get '/' => sub {
    my $c = shift;
    $c->render(text => 'Servidor Mojolicious activo y corriendo dentro del contenedor Docker');
};

#load_data_to_mongo();
app->log->level('debug');

app->start;
