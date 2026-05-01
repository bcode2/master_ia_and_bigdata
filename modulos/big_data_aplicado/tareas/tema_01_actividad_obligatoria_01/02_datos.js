// Insertar los datos de los clientes en la base de datos

db.clientes.insertMany([
    {
        cliente_id: 1,
        nombre: "Ana Ruiz",
        edad: 27,
        ciudad: "Madrid",
        compras: [
            { producto: "Portátil", precio: 899.99, fecha: "2026-03-10" }
        ]
    },
    {
        cliente_id: 2,
        nombre: "Luis Gomez",
        edad: 34,
        ciudad: "Barcelona",
        compras: [
            { producto: "Raton", precio: 24.90, fecha: "2026-02-18" },
            { producto: "Altavoces", precio: 79.90, fecha: "2026-03-01" }
        ]
    },
    {
        cliente_id: 3,
        nombre: "Marta Perez",
        edad: 29,
        ciudad: "Valencia",
        compras: [
            { producto: "Monitor", precio: 189.50, fecha: "2026-01-22" }
        ]
    },
    {
        cliente_id: 4,
        nombre: "Carlos Diaz",
        edad: 41,
        ciudad: "Sevilla",
        compras: [
            { producto: "Impresora", precio: 129.00, fecha: "2026-04-02" }
        ]
    },
    {
        cliente_id: 5,
        nombre: "Elena Martin",
        edad: 31,
        ciudad: "Bilbao",
        compras: [
            { producto: "Teclado", precio: 49.95, fecha: "2026-03-27" }
        ]
    }
]);