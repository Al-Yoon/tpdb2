require('dotenv').config();
const prompt = require('prompt-sync')();
const { Pool } = require('pg');
const { MongoClient } = require('mongodb');
const neo4j = require('neo4j-driver');

// conexión a postgres
const pool = new Pool({
  connectionString: process.env.PG_CONNECTION_STRING,
  ssl: { rejectUnauthorized: false }
});

// conexión a mongodb
const mongoClient = new MongoClient(process.env.MONGO_URI);

// conexión a neo4j
const neo4jDriver = neo4j.driver(
  process.env.NEO4J_URI,
  neo4j.auth.basic(process.env.NEO4J_USER, process.env.NEO4J_PASSWORD)
);

// caso 1: ingresos totales por vendedor en un rango
async function ingresosPorVendedor() {
  console.log("\nIngresos totales por vendedor en un rango\n");
  const vendedor = prompt("ID del vendedor: ");
  const desde = prompt("Fecha desde (YYYY-MM-DD): ");
  const hasta = prompt("Fecha hasta (YYYY-MM-DD): ");
  
  const query = `
    SELECT u.nombre AS vendedor, SUM(oi.subtotal) AS ingresos
    FROM productos p
    JOIN usuarios u ON u.id_usuario = p.id_usuario
    JOIN orden_items oi ON oi.id_producto = p.id_producto
    JOIN orden o ON o.id_orden = oi.id_orden
    WHERE u.id_usuario = $1
      AND o.fecha_creacion BETWEEN $2 AND $3
    GROUP BY u.nombre;`;
  
  try {
    const result = await pool.query(query, [vendedor, desde, hasta]);
    console.table(result.rows);
  } catch (error) {
    console.error("Error:", error.message);
  }
}

// caso 2: top categorías por ventas en el último mes
async function topCategoriasPorVentasUltimoMes() {
  console.log("\nTop categorías por ventas en el último mes\n");
  const query = `
    SELECT c.nombre_categoria, SUM(oi.cantidad) AS total_vendidos
    FROM categoria c
    JOIN productos p ON p.id_categoria = c.id_categoria
    JOIN orden_items oi ON oi.id_producto = p.id_producto
    JOIN orden o ON o.id_orden = oi.id_orden
    WHERE o.fecha_creacion >= NOW() - INTERVAL '1 month'
    GROUP BY c.nombre_categoria
    ORDER BY total_vendidos DESC
    LIMIT 5;`;
  
  try {
    const result = await pool.query(query);
    console.table(result.rows);
  } catch (error) {
    console.error("Error:", error.message);
  }
}

// caso 3: productos activos de una categoría
async function productosActivosDeCategoria() {
  console.log("\nProductos activos de una categoría\n");
  const id_categoria = prompt("ID de la categoría: ");
  const query = `
    SELECT titulo, precio, stock, estado
    FROM productos
    WHERE estado = 'activa' AND id_categoria = $1;`;
  
  try {
    const result = await pool.query(query, [id_categoria]);
    console.table(result.rows);
  } catch (error) {
    console.error("Error:", error.message);
  }
}

// caso 4: órdenes pendientes de un vendedor
async function ordenesPendientesDeUsuario() {
  console.log("\nÓrdenes pendientes de un vendedor\n");
  const id_usuario = prompt("ID del vendedor: ");
  const query = `
    SELECT DISTINCT o.id_orden, o.monto, o.estado, o.fecha_creacion
    FROM orden o
    JOIN orden_items oi ON oi.id_orden = o.id_orden
    JOIN productos p ON p.id_producto = oi.id_producto
    WHERE o.estado = 'pendiente' AND p.id_usuario = $1;`;
  
  try {
    const result = await pool.query(query, [id_usuario]);
    console.table(result.rows);
  } catch (error) {
    console.error("Error:", error.message);
  }
}

// caso 5: órdenes por fecha y vendedor
async function ordenesPorFechaYVendedor() {
  console.log("\nÓrdenes por fecha y vendedor\n");
  const id_vend = prompt("ID del vendedor: ");
  const desde = prompt("Desde (YYYY-MM-DD): ");
  const hasta = prompt("Hasta (YYYY-MM-DD): ");
  const query = `
    SELECT DISTINCT o.id_orden, o.monto, o.fecha_creacion, o.estado
    FROM orden o
    JOIN orden_items oi ON oi.id_orden = o.id_orden
    JOIN productos p ON p.id_producto = oi.id_producto
    WHERE p.id_usuario = $1
      AND o.fecha_creacion BETWEEN $2 AND $3
    ORDER BY o.fecha_creacion;`;
  
  try {
    const result = await pool.query(query, [id_vend, desde, hasta]);
    console.table(result.rows);
  } catch (error) {
    console.error("Error:", error.message);
  }
}

// caso 6: ranking de productos más vendidos
async function rankingProductosMasVendidos() {
  console.log("\nRanking de productos más vendidos\n");
  const query = `
    SELECT p.titulo, SUM(oi.cantidad) AS vendidos
    FROM productos p
    JOIN orden_items oi ON oi.id_producto = p.id_producto
    GROUP BY p.titulo
    ORDER BY vendidos DESC
    LIMIT 10;`;
  
  try {
    const result = await pool.query(query);
    console.table(result.rows);
  } catch (error) {
    console.error("Error:", error.message);
  }
}

// caso 7: compradores que compraron en 2 o más categorías distintas
async function compradoresMulticategoria() {
  console.log("\nCompradores que compraron en >=2 categorías\n");
  const query = `
    SELECT u.nombre, COUNT(DISTINCT p.id_categoria) AS categorias_distintas
    FROM orden o
    JOIN orden_items oi ON oi.id_orden = o.id_orden
    JOIN productos p ON oi.id_producto = p.id_producto
    JOIN usuarios u ON u.id_usuario = o.id_usuario
    GROUP BY u.nombre, u.id_usuario
    HAVING COUNT(DISTINCT p.id_categoria) >= 2;`;
  
  try {
    const result = await pool.query(query);
    console.table(result.rows);
  } catch (error) {
    console.error("Error:", error.message);
  }
}
// caso 8: productos más conectados usando neo4j
async function productosMasConectados() {
  console.log("\nProductos más conectados (comprados por más usuarios)\n");
  const session = neo4jDriver.session({ database: 'neo4j' });
  
  try {
    // cuenta cuántos usuarios distintos compraron cada producto
    const result = await session.run(
      `MATCH (u:Usuario)-[:COMPRO]->(p:Producto)
       RETURN p.titulo AS producto, count(DISTINCT u) AS compradores
       ORDER BY compradores DESC
       LIMIT 10`);
    
    if (result.records.length === 0) {
      console.log("No se encontraron productos con compradores");
      return;
    }
    
    // convertir valores neo4j a tipos js
    const datos = result.records.map(r => {
      const compradores = r.get('compradores');
      return {
        producto: r.get('producto'),
        compradores: compradores.toNumber ? compradores.toNumber() : compradores
      };
    });
    
    console.table(datos);
    
  } catch (error) {
    console.error("Error:", error.message);
  } finally {
    await session.close(); 
  }
}

// caso 9: carritos activos en mongodb con nombres de productos desde postgres
async function carritosActivos() {
  console.log("\nCarritos activos en MongoDB\n");
  
  try {
    const db = mongoClient.db('db2');
    const carritos = db.collection('carrito');
    
    const resultado = await carritos.find({}).toArray(); // obtener carritos
    
    if (resultado.length === 0) {
      console.log("No hay carritos activos");
      return;
    }
    
    console.log(`Total de carritos: ${resultado.length}\n`);
    
    // recolectar ids de productos sin duplicados
    const productosIds = [];
    resultado.forEach(carrito => {
      carrito.productos.forEach(p => {
        if (!productosIds.includes(p.producto_id)) productosIds.push(p.producto_id);
      });
    });
    
    // query postgres: obtener título y precio de productos
    const productosQuery = `
      SELECT id_producto, titulo, precio
      FROM productos
      WHERE id_producto = ANY($1::uuid[]);`;
    
    const productosResult = await pool.query(productosQuery, [productosIds]);
    
    // mapear productos por id para lookup rápido
    const productosMap = {};
    productosResult.rows.forEach(p => {
      productosMap[p.id_producto] = { titulo: p.titulo, precio: p.precio };
    });
    
    console.log("Detalle de carritos:\n");
    
    resultado.forEach((carrito, index) => {
      console.log(`\n--- Carrito ${index + 1} ---`);
      console.log(`Usuario: ${carrito.usuario_id}`);
      console.log(`Última actualización: ${new Date(carrito.fecha_actualizacion).toLocaleString('es-AR')}`);
      console.log(`\nProductos:`);
      
      let totalCarrito = 0; // acumulador
      
      carrito.productos.forEach(prod => {
        const info = productosMap[prod.producto_id]; // obtener datos desde postgres
        
        if (info) {
          const subtotal = info.precio * prod.cantidad;
          totalCarrito += subtotal;
          
          console.log(`  ${prod.cantidad}x ${info.titulo} - $${info.precio.toLocaleString('es-AR')} c/u = $${subtotal.toLocaleString('es-AR')}`);
        } else {
          console.log(`  ${prod.cantidad}x Producto ${prod.producto_id} (no encontrado en BD)`);
        }
      });
      
      console.log(`\nTotal del carrito: $${totalCarrito.toLocaleString('es-AR')}`);
    });
    
  } catch (error) {
    console.error("Error:", error.message);
  }
}

// verificar conexiones
async function verificarConexiones() {
  console.log("\n=== Verificando conexiones ===\n");
  
  // verificar PostgreSQL
  try {
    await pool.query('SELECT NOW()');
    console.log("✓ Conexión a PostgreSQL exitosa");
  } catch (error) {
    console.error("✗ Error al conectar con PostgreSQL:", error.message);
    process.exit(1);
  }
  
  // verificar MongoDB
  try {
    if (!mongoClient.topology || mongoClient.topology.isClosed()) {
      await mongoClient.connect();
    }
    await mongoClient.db('admin').command({ ping: 1 });
    console.log("✓ Conexión a MongoDB exitosa");
  } catch (error) {
    console.error("✗ Error al conectar con MongoDB:", error.message);
    process.exit(1);
  }

  
  // verificar Neo4j
  try {
    await neo4jDriver.verifyConnectivity();
    console.log("✓ Conexión a Neo4j exitosa");
  } catch (error) {
    console.error("✗ Error al conectar con Neo4j:", error.message);
    process.exit(1);
  }
  
  console.log("\n=== Todas las conexiones fueron exitosas ===\n");
}

// menú principal
async function mainMenu() {
  let salir = false;
  while (!salir) {
    console.log("\n--- MENÚ CASOS DE USO ---");
    console.log("1. Ingresos totales por vendedor en un rango");
    console.log("2. Top categorías por ventas en el último mes");
    console.log("3. Productos activos de una categoría");
    console.log("4. Órdenes pendientes de un usuario");
    console.log("5. Órdenes por fecha y vendedor");
    console.log("6. Ranking de productos más vendidos");
    console.log("7. Compradores que compraron en >=2 categorías");
    console.log("8. Productos más conectados (Neo4j)");
    console.log("9. Carritos activos (MongoDB + Postgres)");
    console.log("0. Salir");

    const opcion = prompt("\nIngrese número de opción: ");
    switch (opcion) {
      case "1": await ingresosPorVendedor(); break;
      case "2": await topCategoriasPorVentasUltimoMes(); break;
      case "3": await productosActivosDeCategoria(); break;
      case "4": await ordenesPendientesDeUsuario(); break;
      case "5": await ordenesPorFechaYVendedor(); break;
      case "6": await rankingProductosMasVendidos(); break;
      case "7": await compradoresMulticategoria(); break;
      case "8": await productosMasConectados(); break;
      case "9": await carritosActivos(); break;
      case "0":
        salir = true;
        console.log("\nTerminando el programa");
        await neo4jDriver.close();
        await mongoClient.close();
        break;
      default:
        console.log("Opción inválida");
    }
  }
}

// iniciar programa
(async () => {
  await verificarConexiones();
  await mainMenu();
})();
