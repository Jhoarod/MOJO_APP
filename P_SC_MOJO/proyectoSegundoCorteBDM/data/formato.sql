

    CREATE TABLE articulos (
        idArticulo INTEGER PRIMARY KEY,
        nombreArticulo TEXT NOT NULL,
        precioArticulo REAL,
        cantidadArticulo INTEGER
    )

    CREATE TABLE personas (
        numeroDocumento INTEGER PRIMARY KEY,
        nombres TEXT NOT NULL,
        primerApellido TEXT NOT NULL,
        segundoApellido TEXT,
        fechaNacimiento DATE,
        telefono TEXT,
        direccion TEXT,
        email TEXT
    )

    CREATE TABLE ventas (
        idVenta INTEGER PRIMARY KEY,
        idComprador INTEGER NOT NULL,
        idArticulo INTEGER NOT NULL,
        cantidadProductos INTEGER NOT NULL,
        precioTotal DECIMAL(10, 2) NOT NULL,
        FOREIGN KEY (idComprador) REFERENCES personas (numeroDocumento),
        FOREIGN KEY (idArticulo) REFERENCES articulos (idArticulo)
    )
