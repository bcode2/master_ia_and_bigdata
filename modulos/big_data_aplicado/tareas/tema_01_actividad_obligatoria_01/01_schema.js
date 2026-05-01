// Creación de la colección en la bbdd de mongodb

db.createCollection("clientes", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["cliente_id", "nombre", "edad", "ciudad", "compras"],
            properties: {
                cliente_id: { bsonType: "int", minimum: 1 },
                nombre: { bsonType: "string", minLength: 1 },
                edad: { bsonType: "int", minimum: 0, maximum: 120 },
                ciudad: { bsonType: "string", minLength: 1 },
                compras: {
                    bsonType: "array",
                    minItems: 1,
                    items: {
                        bsonType: "object",
                        required: ["producto", "precio", "fecha"],
                        properties: {
                            producto: { bsonType: "string", minLength: 1 },
                            precio: { bsonType: ["double", "int", "long", "decimal"], minimum: 0 },
                            fecha: { bsonType: "string" }
                        }
                    }
                }
            }
        }
    }
});