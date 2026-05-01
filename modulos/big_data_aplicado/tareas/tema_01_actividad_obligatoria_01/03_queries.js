// 3.4 Consultas y sus explicaciones

// Consulta 1: Clientes con compras > 100 EUR
/*
Explicación:
1) Se busca en el array "compras" cualquier elemento con precio mayor a 100.
2) MongoDB hace match si al menos una compra del cliente cumple la condicion.
3) Se proyectan datos basicos para identificar rapidamente al cliente.
*/
db.clientes.find(
  { "compras.precio": { $gt: 100 } },
  { _id: 0, cliente_id: 1, nombre: 1, ciudad: 1, compras: 1 }
).pretty();

// Consulta 2: Nombre y ciudad de compradores de "Portatil"
/*
Explicación:
1) El filtro revisa el campo "compras.producto" dentro del array de compras.
2) Solo devuelve clientes que hayan comprado exactamente "Portátil".
3) La proyección limita salida a "nombre" y "ciudad" para cumplir el enunciado.
*/
db.clientes.find(
  { "compras.producto": "Portátil" },
  { _id: 0, nombre: 1, ciudad: 1 }
).pretty();

// Consulta 3: Clientes de Madrid con edad > 30
/*
Explicación:
1) Se combinan dos criterios en el mismo documento de filtro: ciudad y edad.
2) Solo pasan los clientes cuya ciudad sea "Madrid" y edad mayor que 30.3
*/
db.clientes.find(
  { ciudad: "Madrid", edad: { $gt: 30 } },
  { _id: 0, cliente_id: 1, nombre: 1, edad: 1, ciudad: 1 }
).pretty();

// Consulta 4: Compras del cliente más joven
/*
Explicación:
1) Se ordena por "edad" ascendente para que el primer documento sea el mas joven.
2) Con limit(1) se devuelve solo ese cliente.
3) El resultado muestra su nombre, edad y el array completo de compras.
*/
db.clientes.find(
  {},
  { _id: 0, nombre: 1, edad: 1, compras: 1 }
).sort({ edad: 1 }).limit(1).pretty();

// Consulta 5: Clientes con al menos dos compras
/*
Explicación:
1) Se usa $expr para evaluar el tamano del array "compras" en cada documento.
2) $size obtiene el número de compras y $gte exige mínimo 2.
3) Se muestrsa también "total_compras" para justificar el resultado.
*/
db.clientes.aggregate([
  {
    $match: {
      $expr: { $gte: [{ $size: "$compras" }, 2] }
    }
  },
  {
    $project: {
      _id: 0,
      cliente_id: 1,
      nombre: 1,
      ciudad: 1,
      total_compras: { $size: "$compras" }
    }
  }
]);
