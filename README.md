# LunaDB client Go Repository

## Code example

```Go
/* ==========================================================
   Path and File: ./vs.go
   ========================================================== */

package main

import (
 "database/sql"
 "encoding/json"
 "fmt"
 "log"
 "math/rand"
 "os"
 "strings"
 "sync"
 "sync/atomic"
 "text/tabwriter"
 "time"

 mtclient "github.com/adoboscan21/lunadb-client-go"
 _ "github.com/go-sql-driver/mysql" // MariaDB/MySQL Driver
 _ "github.com/lib/pq"              // PostgreSQL Driver
 "go.mongodb.org/mongo-driver/bson"
)

// Constantes de simulación masiva (Enterprise ERP Peak)
const (
 TotalOrdersHistory = 1000000 // 🔥 1,000,000 Pedidos históricos en la BD
 ConcurrentTx       = 10000   // 10,000 Transacciones/Lecturas Concurrentes (Fiebre de Quincena)
)

// Metrics almacena los tiempos de ejecución
type Metrics struct {
 EngineName         string
 Connection         time.Duration
 Schema             time.Duration
 Insert             time.Duration
 OrderRead          time.Duration
 PriceAdjustment    time.Duration
 DeleteOrder        time.Duration
 HighValueQuery     time.Duration
 FraudDetection     time.Duration
 SearchCustomer     time.Duration
 TopOrdersRanking   time.Duration
 DeepPagination     time.Duration
 DistinctQuantities time.Duration
 FullInvoiceJoin    time.Duration
 SalesByBranch      time.Duration
 GlobalRevenue      time.Duration
 MassDispatch       time.Duration
 BulkAdjustment     time.Duration
 PurgeOldOrders     time.Duration
 PaymentACIDTx      time.Duration
 QuincenaRush       time.Duration
 Cleanup            time.Duration
}

func printJSON(data any) {
 pretty, _ := json.MarshalIndent(data, "      ", "  ")
 fmt.Printf("      %s\n", string(pretty))
}

// ---------------------------------------------------------
// HELPERS PARA VALIDACIÓN ESTRICTA Y VISUALIZACIÓN DE DATOS
// ---------------------------------------------------------

// expectResp envuelve la verificación básica.
func expectResp(phase string) func(*mtclient.CommandResponse, error) {
 return func(resp *mtclient.CommandResponse, err error) {
  if err != nil {
   log.Fatalf("❌ [%s] Error de red/cliente: %v", phase, err)
  }
  if resp == nil {
   log.Fatalf("❌ [%s] Error: Respuesta nula del servidor", phase)
  }
  if !resp.OK() {
   log.Fatalf("❌ [%s] Error del motor: %s - %s", phase, resp.Status, resp.Message)
  }
 }
}

// expectMutation verifica y muestra un resumen de las mutaciones devueltas.
func expectMutation(phase string) func(*mtclient.CommandResponse, error) {
 return func(resp *mtclient.CommandResponse, err error) {
  expectResp(phase)(resp, err)

  fmt.Printf("      └─ ✅ %s\n", resp.Message)

  // Imprimir contenido crudo si existe (como en una lectura directa)
  if len(resp.RawData) > 0 {
   var doc map[string]any
   if err := bson.Unmarshal(resp.RawData, &doc); err == nil {
    // Evitar imprimir arrays masivos en consola
    if _, hasArray := doc["array"]; !hasArray {
     printJSON(doc)
    }
   }
  }
 }
}

// expectQuery verifica y muestra el contenido real de los resultados de una Query.
func expectQuery(phase string) func(*mtclient.CommandResponse, error) {
 return func(resp *mtclient.CommandResponse, err error) {
  expectResp(phase)(resp, err)

  var resultWrapper struct {
   Results any `bson:"results"`
  }
  if err := bson.Unmarshal(resp.RawData, &resultWrapper); err != nil {
   log.Fatalf("❌ [%s] Falló decodificación BSON: %v", phase, err)
  }

  switch v := resultWrapper.Results.(type) {
  case bson.A: // Array de documentos
   if len(v) == 0 {
    log.Fatalf("❌ [%s] FALSO POSITIVO: La consulta ejecutó, pero devolvió 0 datos.", phase)
   }
   fmt.Printf("      └─ ✅ Éxito: Se encontraron %d registros.\n", len(v))
   fmt.Printf("      └─ 📄 Muestra del primer registro:\n")
   printJSON(v[0])
  case bson.M: // Mapa simple (usado en Counts)
   fmt.Printf("      └─ ✅ Éxito: Resultado de Agregación:\n")
   printJSON(v)
  default: // Otros tipos (ej. lista plana de valores)
   fmt.Printf("      └─ ✅ Éxito: Se devolvieron valores simples (Muestra: %v)\n", v)
  }
 }
}

func main() {
 fmt.Println("=========================================================================================")
 fmt.Printf(" 🏭 ALIMENTOS POLAR ERP SIMULATOR: LunaDB vs MARIADB vs POSTGRESQL (%dK DB, %dk CCU) 🏭 \n", TotalOrdersHistory/1000, ConcurrentTx/1000)
 fmt.Println("=========================================================================================")

 mtMetrics := runLunaDB()
 mariaMetrics := runMariaDB()
 pgMetrics := runPostgreSQL()

 printComparison(mtMetrics, mariaMetrics, pgMetrics)
}

// ==========================================
// ENGINE 1: LunaDB
// ==========================================
func runLunaDB() Metrics {
 fmt.Println("\n>>> [1/3] BOOTING ENTERPRISE ERP WITH LunaDB <<<")
 var m Metrics
 m.EngineName = "LunaDB"

 fmt.Println("⏳ [Phase 0] Boot: Connecting to Main Database...")
 start := time.Now()
 client := mtclient.NewClient(mtclient.ClientOptions{
  Host:               "localhost",
  Port:               5876,
  Username:           "admin",
  Password:           "adminpass",
  InsecureSkipVerify: true,
  PoolSize:           100,
 })

 if err := client.Connect(); err != nil {
  log.Fatalf("❌ [MT FAIL] Connection error: %v", err)
 }
 defer client.Close()
 m.Connection = time.Since(start)

 colOrders := fmt.Sprintf("polar_orders_%d", time.Now().UnixNano())
 colBranches := fmt.Sprintf("polar_branches_%d", time.Now().UnixNano())
 colProducts := fmt.Sprintf("polar_products_%d", time.Now().UnixNano())

 fmt.Printf("⏳ [Phase 1] Init: Creating Financial Schema and Indexes...\n")
 start = time.Now()
 expectResp("CollectionCreate Orders")(client.CollectionCreate(colOrders))
 expectResp("CollectionCreate Branches")(client.CollectionCreate(colBranches))
 expectResp("CollectionCreate Products")(client.CollectionCreate(colProducts))

 indexes := []string{"quantity", "total_bs", "usd_amount", "branch_id", "product_id", "status", "status,quantity", "customer_name"}
 for _, idx := range indexes {
  expectResp("IndexCreate " + idx)(client.CollectionIndexCreate(colOrders, idx))
 }
 m.Schema = time.Since(start)

 fmt.Printf("⏳ [Phase 2] Data Gen: Inserting %d Historical Orders (Batches of 5000)...\n", TotalOrdersHistory)
 start = time.Now()

 branches := []string{"APC Los Cortijos", "APC Valencia", "APC Maracaibo", "APC Barquisimeto", "APC Puerto La Cruz", "APC San Cristobal", "APC Guayana"}
 branchesData := make([]any, len(branches))
 for i, b := range branches {
  branchesData[i] = map[string]any{"_id": fmt.Sprintf("branch_%d", i), "name": b, "operating_budget": float64(1000000)}
 }
 expectResp("Insert Branches")(client.CollectionItemSetMany(colBranches, branchesData))

 products := []string{"Harina P.A.N.", "Maltín Polar", "Cerveza Polar Pilsen", "Toddy", "Mantequilla Mavesa"}
 productData := make([]any, len(products))
 for i, p := range products {
  productData[i] = map[string]any{"_id": fmt.Sprintf("prod_%d", i), "name": p}
 }
 expectResp("Insert Products")(client.CollectionItemSetMany(colProducts, productData))

 batchSize := 5000
 statuses := []string{"Pending", "Processed", "Shipped", "Cancelled"}

 for i := 0; i < TotalOrdersHistory; i += batchSize {
  batch := make([]any, 0, batchSize)
  for j := 0; j < batchSize; j++ {
   id := i + j
   batch = append(batch, map[string]any{
    "_id":           fmt.Sprintf("order_%d", id),
    "customer_name": fmt.Sprintf("Supermercado_%d", id),
    "quantity":      int32(rand.Intn(500) + 10),
    "total_bs":      float64(rand.Intn(5000000)),
    "usd_amount":    float64(rand.Intn(10000)),
    "branch_id":     fmt.Sprintf("branch_%d", rand.Intn(len(branches))),
    "product_id":    fmt.Sprintf("prod_%d", rand.Intn(len(products))),
    "status":        statuses[rand.Intn(len(statuses))],
   })
  }
  expectResp(fmt.Sprintf("Insert Batch %d", i))(client.CollectionItemSetMany(colOrders, batch))
 }
 m.Insert = time.Since(start)

 fmt.Printf("⏳ [Phase 3] Action: Concurrent Order Lookups (%d requests)...\n", ConcurrentTx)
 start = time.Now()
 var successCount, errorCount int32
 var wg sync.WaitGroup
 wg.Add(ConcurrentTx)
 for i := 0; i < ConcurrentTx; i++ {
  go func() {
   defer wg.Done()
   res, err := client.CollectionItemGet(colOrders, fmt.Sprintf("order_%d", rand.Intn(TotalOrdersHistory)))
   if err == nil && res.Found() {
    atomic.AddInt32(&successCount, 1)
   } else {
    atomic.AddInt32(&errorCount, 1)
   }
  }()
 }
 wg.Wait()
 m.OrderRead = time.Since(start) / time.Duration(ConcurrentTx)
 fmt.Printf("      └─ ✅ Completadas: %d lecturas.\n", successCount)

 fmt.Printf("⏳ [Phase 4] Action: Mass Price Inflation Adjustments (%d updates)...\n", ConcurrentTx)
 start = time.Now()
 successCount, errorCount = 0, 0
 wg.Add(ConcurrentTx)
 for i := 0; i < ConcurrentTx; i++ {
  go func() {
   defer wg.Done()
   res, err := client.CollectionItemUpdate(colOrders, fmt.Sprintf("order_%d", rand.Intn(TotalOrdersHistory)), map[string]any{"total_bs": float64(9999999)})
   if err == nil && res.OK() {
    atomic.AddInt32(&successCount, 1)
   } else {
    atomic.AddInt32(&errorCount, 1)
   }
  }()
 }
 wg.Wait()
 m.PriceAdjustment = time.Since(start) / time.Duration(ConcurrentTx)
 fmt.Printf("      └─ ✅ Completadas: %d actualizaciones.\n", successCount)

 fmt.Println("⏳ [Phase 5] Action: Cancel/Delete Specific Order...")
 start = time.Now()
 expectMutation("Delete Order")(client.CollectionItemDelete(colOrders, "order_50001"))
 m.DeleteOrder = time.Since(start)

 fmt.Println("⏳ [Phase 6] Report: High Value Processed Orders (qty 400-500, Processed, Top USD)...")
 start = time.Now()
 limit5 := 5
 qDeep := mtclient.Query{
  Filter: map[string]any{
   "and": []any{
    map[string]any{"field": "status", "op": "=", "value": "Processed"},
    map[string]any{"field": "quantity", "op": "between", "value": []any{int32(400), int32(500)}},
   },
  },
  OrderBy: []any{map[string]any{"field": "usd_amount", "direction": "desc"}},
  Limit:   &limit5,
 }
 expectQuery("HighValueQuery")(client.CollectionQuery(colOrders, qDeep))
 m.HighValueQuery = time.Since(start)

 fmt.Println("⏳ [Phase 7] Audit: Fraud Detection (Processed with qty < 50 OR USD > 9000)...")
 start = time.Now()
 qComplex := mtclient.Query{
  Filter: map[string]any{
   "or": []any{
    map[string]any{
     "and": []any{
      map[string]any{"field": "status", "op": "=", "value": "Processed"},
      map[string]any{"field": "quantity", "op": "<", "value": int32(50)},
     },
    },
    map[string]any{"field": "usd_amount", "op": ">", "value": float64(9000)},
   },
  },
  Limit: &limit5,
 }
 expectQuery("FraudDetection")(client.CollectionQuery(colOrders, qComplex))
 m.FraudDetection = time.Since(start)

 fmt.Println("⏳ [Phase 8] Support: Search Customer (name LIKE 'Supermercado_777%')...")
 start = time.Now()
 qWildcard := mtclient.Query{
  Filter: map[string]any{"field": "customer_name", "op": "like", "value": "Supermercado_777%"},
  Limit:  &limit5,
 }
 expectQuery("SearchCustomer")(client.CollectionQuery(colOrders, qWildcard))
 m.SearchCustomer = time.Since(start)

 fmt.Println("⏳ [Phase 9] Report: Top 10 Largest Orders by Quantity (Offset 1000)...")
 start = time.Now()
 limit10 := 10
 qPag := mtclient.Query{
  OrderBy: []any{map[string]any{"field": "quantity", "direction": "desc"}},
  Limit:   &limit10,
  Offset:  1000,
 }
 expectQuery("TopOrdersRanking")(client.CollectionQuery(colOrders, qPag))
 m.TopOrdersRanking = time.Since(start)

 fmt.Println("⏳ [Phase 10] Report: Deep Pagination by USD Value (Offset 500k)...")
 start = time.Now()
 qDeepPag := mtclient.Query{
  OrderBy: []any{map[string]any{"field": "usd_amount", "direction": "asc"}},
  Limit:   &limit5,
  Offset:  500000,
 }
 expectQuery("DeepPagination")(client.CollectionQuery(colOrders, qDeepPag))
 m.DeepPagination = time.Since(start)

 fmt.Println("⏳ [Phase 11] BI: Distinct Order Quantities...")
 start = time.Now()
 expectQuery("DistinctQuantities")(client.CollectionQuery(colOrders, mtclient.Query{Distinct: "quantity"}))
 m.DistinctQuantities = time.Since(start)

 fmt.Println("⏳ [Phase 12] ERP: View Full Invoice Details (Multi-Join Branch/Product)...")
 start = time.Now()
 qJoin := mtclient.Query{
  Filter: map[string]any{"field": "customer_name", "op": "like", "value": "Supermercado_999%"},
  Lookups: []any{
   map[string]any{"from": colBranches, "localField": "branch_id", "foreignField": "_id", "as": "branch_details"},
   map[string]any{"from": colProducts, "localField": "product_id", "foreignField": "_id", "as": "product_details"},
  },
  Limit: &limit5,
 }
 expectQuery("FullInvoiceJoin")(client.CollectionQuery(colOrders, qJoin))
 m.FullInvoiceJoin = time.Since(start)

 fmt.Println("⏳ [Phase 13] BI: Total Orders by Branch (Simple Aggregation)...")
 start = time.Now()
 qAgg1 := mtclient.Query{
  GroupBy:      []string{"branch_id"},
  Aggregations: map[string]any{"total_orders": map[string]any{"func": "count", "field": "*"}},
 }
 expectQuery("SalesByBranch")(client.CollectionQuery(colOrders, qAgg1))
 m.SalesByBranch = time.Since(start)

 fmt.Println("⏳ [Phase 14] BI: Global USD Revenue by Branch and Status (Multi-Aggregation)...")
 start = time.Now()
 qAgg2 := mtclient.Query{
  GroupBy: []string{"branch_id", "status"},
  Aggregations: map[string]any{
   "total_orders": map[string]any{"func": "count", "field": "*"},
   "sum_usd":      map[string]any{"func": "sum", "field": "usd_amount"},
  },
 }
 expectQuery("GlobalRevenue")(client.CollectionQuery(colOrders, qAgg2))
 m.GlobalRevenue = time.Since(start)

 fmt.Println("⏳ [Phase 15] Logistics: Mass Dispatch (100 Pending -> Shipped)...")
 start = time.Now()
 limit100 := 100
 respPending, err := client.CollectionQuery(colOrders, mtclient.Query{
  Filter: map[string]any{"field": "status", "op": "=", "value": "Pending"}, Limit: &limit100, Projection: []string{"_id"},
 })
 expectResp("Fetch Pending")(respPending, err)

 var pendingOrders []map[string]any
 respPending.UnmarshalResults(&pendingOrders)
 bulkModify := make([]any, len(pendingOrders))
 for i, o := range pendingOrders {
  bulkModify[i] = map[string]any{"_id": o["_id"], "patch": map[string]any{"status": "Shipped"}}
 }
 expectMutation("MassDispatch")(client.CollectionItemUpdateMany(colOrders, bulkModify))
 m.MassDispatch = time.Since(start)

 fmt.Println("⏳ [Phase 16] Admin: Bulk Quantity Adjustment for VIP Orders...")
 start = time.Now()
 expectMutation("Bulk Adjustment")(client.CollectionItemUpdateMany(colOrders, []any{
  map[string]any{"_id": "order_10", "patch": map[string]any{"quantity": int32(5000)}},
  map[string]any{"_id": "order_20", "patch": map[string]any{"quantity": int32(5000)}},
 }))
 m.BulkAdjustment = time.Since(start)

 fmt.Println("⏳ [Phase 17] Maintenance: Purge 10,000 Old/Cancelled Orders...")
 start = time.Now()
 oldOrders := make([]string, 10000)
 for i := 0; i < 10000; i++ {
  oldOrders[i] = fmt.Sprintf("order_%d", TotalOrdersHistory-i-1)
 }
 expectMutation("PurgeOldOrders")(client.CollectionItemDeleteMany(colOrders, oldOrders))
 m.PurgeOldOrders = time.Since(start)

 fmt.Println("⏳ [Phase 18] Finance: Payment Processing (ACID Transaction)...")
 start = time.Now()
 tx, err := client.Begin()
 if err != nil {
  log.Fatalf("❌ Fallo al iniciar TX: %v", err)
 }
 _, errTx := tx.CollectionItemUpdate(colOrders, "order_10", map[string]any{"usd_amount": 0, "status": "Processed"})
 if errTx != nil {
  log.Fatalf("❌ Error en actualización dentro de TX: %v", errTx)
 }
 expectMutation("TX Rollback")(tx.Rollback())
 m.PaymentACIDTx = time.Since(start)

 fmt.Printf("⏳ [Phase 19] Stress: Quincena Rush (%d concurrent Ops)...\n", ConcurrentTx)
 start = time.Now()
 successCount, errorCount = 0, 0
 wg.Add(ConcurrentTx)
 for i := 0; i < ConcurrentTx; i++ {
  go func(txID int) {
   defer wg.Done()
   targetOrder := fmt.Sprintf("order_%d", rand.Intn(TotalOrdersHistory-10000))
   if txID%2 == 0 {
    res, err := client.CollectionItemGet(colOrders, targetOrder)
    if err == nil && res.Found() {
     atomic.AddInt32(&successCount, 1)
    } else {
     atomic.AddInt32(&errorCount, 1)
    }
   } else {
    res, err := client.CollectionItemUpdate(colOrders, targetOrder, map[string]any{"status": "Processed"})
    if err == nil && res.OK() {
     atomic.AddInt32(&successCount, 1)
    } else {
     atomic.AddInt32(&errorCount, 1)
    }
   }
  }(i)
 }
 wg.Wait()
 m.QuincenaRush = time.Since(start)
 fmt.Printf("      └─ ✅ Completadas: %d operaciones en Rush.\n", successCount)

 fmt.Println("⏳ [Phase 20] Shutdown: Drop ERP Tables...")
 start = time.Now()
 expectResp("Drop Orders")(client.CollectionDelete(colOrders))
 expectResp("Drop Branches")(client.CollectionDelete(colBranches))
 expectResp("Drop Products")(client.CollectionDelete(colProducts))
 m.Cleanup = time.Since(start)

 return m
}

// ==========================================
// ENGINE 2: MARIADB
// ==========================================
func runMariaDB() Metrics {
 fmt.Println("\n>>> [2/3] BOOTING ENTERPRISE ERP WITH MARIADB <<<")
 var m Metrics
 m.EngineName = "MariaDB"

 start := time.Now()
 dbInit, _ := sql.Open("mysql", "root:12345678@tcp(127.0.0.1:3306)/?multiStatements=true")
 dbInit.Ping()
 m.Connection = time.Since(start)

 dbName := fmt.Sprintf("polar_db_%d", time.Now().UnixNano())
 dbInit.Exec(fmt.Sprintf("CREATE DATABASE %s;", dbName))
 dbInit.Close()
 db, _ := sql.Open("mysql", fmt.Sprintf("root:12345678@tcp(127.0.0.1:3306)/%s?multiStatements=true", dbName))
 defer db.Close()
 db.SetMaxOpenConns(100)

 createTables := `
  CREATE TABLE branches (_id VARCHAR(50) PRIMARY KEY, name VARCHAR(100), operating_budget DOUBLE);
  CREATE TABLE products (_id VARCHAR(50) PRIMARY KEY, name VARCHAR(100));
  CREATE TABLE orders (
   _id VARCHAR(50) PRIMARY KEY, customer_name VARCHAR(100), quantity INT, total_bs DOUBLE, usd_amount DOUBLE, branch_id VARCHAR(50), product_id VARCHAR(50), status VARCHAR(20),
   INDEX idx_qty (quantity), INDEX idx_bs (total_bs), INDEX idx_usd (usd_amount), INDEX idx_branch (branch_id), INDEX idx_prod (product_id), INDEX idx_status (status), INDEX idx_name(customer_name),
   INDEX idx_status_qty (status, quantity)
  );
 `
 start = time.Now()
 db.Exec(createTables)
 m.Schema = time.Since(start)

 start = time.Now()
 for i := 0; i < 7; i++ {
  db.Exec("INSERT INTO branches VALUES (?, ?, ?)", fmt.Sprintf("branch_%d", i), "Sucursal", 1000000.0)
 }
 for i := 0; i < 5; i++ {
  db.Exec("INSERT INTO products VALUES (?, ?)", fmt.Sprintf("prod_%d", i), "Producto")
 }
 statuses := []string{"Pending", "Processed", "Shipped", "Cancelled"}
 for i := 0; i < TotalOrdersHistory; i += 5000 {
  vals, args := []string{}, []any{}
  for j := 0; j < 5000; j++ {
   id := i + j
   vals = append(vals, "(?, ?, ?, ?, ?, ?, ?, ?)")
   args = append(args, fmt.Sprintf("order_%d", id), fmt.Sprintf("Supermercado_%d", id), rand.Intn(500)+10, float64(rand.Intn(5000000)), float64(rand.Intn(10000)), fmt.Sprintf("branch_%d", rand.Intn(7)), fmt.Sprintf("prod_%d", rand.Intn(5)), statuses[rand.Intn(4)])
  }
  db.Exec(fmt.Sprintf("INSERT INTO orders VALUES %s", strings.Join(vals, ",")), args...)
 }
 m.Insert = time.Since(start)

 var dump string

 fmt.Printf("⏳ [Phase 3] Action: Concurrent Order Lookups (%d requests)...\n", ConcurrentTx)
 start = time.Now()
 var wg3 sync.WaitGroup
 wg3.Add(ConcurrentTx)
 for i := 0; i < ConcurrentTx; i++ {
  go func() {
   defer wg3.Done()
   var localDump string
   db.QueryRow("SELECT _id FROM orders WHERE _id = ?", fmt.Sprintf("order_%d", rand.Intn(TotalOrdersHistory))).Scan(&localDump)
  }()
 }
 wg3.Wait()
 m.OrderRead = time.Since(start) / time.Duration(ConcurrentTx)

 fmt.Printf("⏳ [Phase 4] Action: Mass Price Inflation Adjustments (%d updates)...\n", ConcurrentTx)
 start = time.Now()
 var wg4 sync.WaitGroup
 wg4.Add(ConcurrentTx)
 for i := 0; i < ConcurrentTx; i++ {
  go func() {
   defer wg4.Done()
   db.Exec("UPDATE orders SET total_bs = 9999999 WHERE _id = ?", fmt.Sprintf("order_%d", rand.Intn(TotalOrdersHistory)))
  }()
 }
 wg4.Wait()
 m.PriceAdjustment = time.Since(start) / time.Duration(ConcurrentTx)

 start = time.Now()
 db.Exec("DELETE FROM orders WHERE _id = 'order_50001'")
 m.DeleteOrder = time.Since(start)

 start = time.Now()
 r, _ := db.Query("SELECT _id FROM orders WHERE status = 'Processed' AND quantity BETWEEN 400 AND 500 ORDER BY usd_amount DESC LIMIT 5")
 for r.Next() {
  r.Scan(&dump)
 }
 r.Close()
 m.HighValueQuery = time.Since(start)

 start = time.Now()
 rc, _ := db.Query("SELECT _id FROM orders WHERE (status = 'Processed' AND quantity < 50) OR usd_amount > 9000 LIMIT 5")
 for rc.Next() {
  rc.Scan(&dump)
 }
 rc.Close()
 m.FraudDetection = time.Since(start)

 start = time.Now()
 rw, _ := db.Query("SELECT _id FROM orders WHERE customer_name LIKE 'Supermercado_777%' LIMIT 5")
 for rw.Next() {
  rw.Scan(&dump)
 }
 rw.Close()
 m.SearchCustomer = time.Since(start)

 start = time.Now()
 rp, _ := db.Query("SELECT _id FROM orders ORDER BY quantity DESC LIMIT 10 OFFSET 1000")
 for rp.Next() {
  rp.Scan(&dump)
 }
 rp.Close()
 m.TopOrdersRanking = time.Since(start)

 fmt.Println("⏳ [Phase 10] Report: Deep Pagination by USD Value (Offset 500k)...")
 start = time.Now()
 rdp, _ := db.Query("SELECT _id FROM orders ORDER BY usd_amount ASC LIMIT 5 OFFSET 500000")
 for rdp.Next() {
  rdp.Scan(&dump)
 }
 rdp.Close()
 m.DeepPagination = time.Since(start)

 start = time.Now()
 rdis, _ := db.Query("SELECT DISTINCT quantity FROM orders")
 for rdis.Next() {
  rdis.Scan(&dump)
 }
 rdis.Close()
 m.DistinctQuantities = time.Since(start)

 start = time.Now()
 rj, _ := db.Query("SELECT o._id FROM orders o LEFT JOIN branches b ON o.branch_id = b._id LEFT JOIN products p ON o.product_id = p._id WHERE o.customer_name LIKE 'Supermercado_999%' LIMIT 5")
 for rj.Next() {
  rj.Scan(&dump)
 }
 rj.Close()
 m.FullInvoiceJoin = time.Since(start)

 start = time.Now()
 rag, _ := db.Query("SELECT branch_id, COUNT(*) FROM orders GROUP BY branch_id")
 for rag.Next() {
  rag.Scan(&dump, &dump)
 }
 rag.Close()
 m.SalesByBranch = time.Since(start)

 start = time.Now()
 ram, _ := db.Query("SELECT branch_id, status, COUNT(*), SUM(usd_amount) FROM orders GROUP BY branch_id, status")
 for ram.Next() {
  ram.Scan(&dump, &dump, &dump, &dump)
 }
 ram.Close()
 m.GlobalRevenue = time.Since(start)

 start = time.Now()
 rfm, _ := db.Query("SELECT _id FROM orders WHERE status = 'Pending' LIMIT 100")
 var ids []string
 for rfm.Next() {
  var id string
  rfm.Scan(&id)
  ids = append(ids, "'"+id+"'")
 }
 rfm.Close()
 if len(ids) > 0 {
  db.Exec(fmt.Sprintf("UPDATE orders SET status = 'Shipped' WHERE _id IN (%s)", strings.Join(ids, ",")))
 }
 m.MassDispatch = time.Since(start)

 start = time.Now()
 db.Exec("UPDATE orders SET quantity = 5000 WHERE _id IN ('order_10', 'order_20')")
 m.BulkAdjustment = time.Since(start)

 start = time.Now()
 botKeys := make([]string, 10000)
 for i := 0; i < 10000; i++ {
  botKeys[i] = fmt.Sprintf("'order_%d'", TotalOrdersHistory-i-1)
 }
 db.Exec(fmt.Sprintf("DELETE FROM orders WHERE _id IN (%s)", strings.Join(botKeys, ",")))
 m.PurgeOldOrders = time.Since(start)

 start = time.Now()
 tx, _ := db.Begin()
 tx.Exec("UPDATE orders SET usd_amount = 0 WHERE _id = 'order_10'")
 tx.Rollback()
 m.PaymentACIDTx = time.Since(start)

 start = time.Now()
 var wg sync.WaitGroup
 wg.Add(ConcurrentTx)
 for i := 0; i < ConcurrentTx; i++ {
  go func(txID int) {
   defer wg.Done()
   targetOrder := fmt.Sprintf("order_%d", rand.Intn(TotalOrdersHistory-10000))
   if txID%2 == 0 {
    db.QueryRow("SELECT _id FROM orders WHERE _id = ?", targetOrder).Scan(&dump)
   } else {
    db.Exec("UPDATE orders SET status = 'Processed' WHERE _id = ?", targetOrder)
   }
  }(i)
 }
 wg.Wait()
 m.QuincenaRush = time.Since(start)

 start = time.Now()
 db.Exec(fmt.Sprintf("DROP DATABASE %s", dbName))
 m.Cleanup = time.Since(start)

 return m
}

// ==========================================
// ENGINE 3: POSTGRESQL
// ==========================================
func runPostgreSQL() Metrics {
 fmt.Println("\n>>> [3/3] BOOTING ENTERPRISE ERP WITH POSTGRESQL <<<")
 var m Metrics
 m.EngineName = "PostgreSQL"

 start := time.Now()
 dbInit, err := sql.Open("postgres", "postgres://postgres:12345678@localhost:5432/postgres?sslmode=disable")
 if err != nil {
  log.Fatalf("❌ [PG FAIL] Connection error: %v", err)
 }
 dbInit.Ping()
 m.Connection = time.Since(start)

 dbName := fmt.Sprintf("polar_db_%d", time.Now().UnixNano())
 dbInit.Exec(fmt.Sprintf("CREATE DATABASE %s;", dbName))
 dbInit.Close()

 db, _ := sql.Open("postgres", fmt.Sprintf("postgres://postgres:12345678@localhost:5432/%s?sslmode=disable", dbName))
 defer db.Close()
 db.SetMaxOpenConns(100)

 createTables := `
  CREATE TABLE branches (_id VARCHAR(50) PRIMARY KEY, name VARCHAR(100), operating_budget DOUBLE PRECISION);
  CREATE TABLE products (_id VARCHAR(50) PRIMARY KEY, name VARCHAR(100));
  CREATE TABLE orders (
   _id VARCHAR(50) PRIMARY KEY, customer_name VARCHAR(100), quantity INT, total_bs DOUBLE PRECISION, usd_amount DOUBLE PRECISION, branch_id VARCHAR(50), product_id VARCHAR(50), status VARCHAR(20)
  );
  CREATE INDEX idx_qty ON orders(quantity); CREATE INDEX idx_bs ON orders(total_bs); CREATE INDEX idx_usd ON orders(usd_amount);
  CREATE INDEX idx_branch ON orders(branch_id); CREATE INDEX idx_prod ON orders(product_id); CREATE INDEX idx_status ON orders(status); CREATE INDEX idx_name ON orders(customer_name);
  CREATE INDEX idx_status_qty ON orders(status, quantity);
 `
 start = time.Now()
 db.Exec(createTables)
 m.Schema = time.Since(start)

 start = time.Now()
 for i := 0; i < 7; i++ {
  db.Exec("INSERT INTO branches VALUES ($1, $2, $3)", fmt.Sprintf("branch_%d", i), "Sucursal", 1000000.0)
 }
 for i := 0; i < 5; i++ {
  db.Exec("INSERT INTO products VALUES ($1, $2)", fmt.Sprintf("prod_%d", i), "Producto")
 }
 statuses := []string{"Pending", "Processed", "Shipped", "Cancelled"}

 for i := 0; i < TotalOrdersHistory; i += 5000 {
  vals, args := []string{}, []any{}
  for j := 0; j < 5000; j++ {
   id := i + j
   vals = append(vals, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)", j*8+1, j*8+2, j*8+3, j*8+4, j*8+5, j*8+6, j*8+7, j*8+8))
   args = append(args, fmt.Sprintf("order_%d", id), fmt.Sprintf("Supermercado_%d", id), rand.Intn(500)+10, float64(rand.Intn(5000000)), float64(rand.Intn(10000)), fmt.Sprintf("branch_%d", rand.Intn(7)), fmt.Sprintf("prod_%d", rand.Intn(5)), statuses[rand.Intn(4)])
  }
  db.Exec(fmt.Sprintf("INSERT INTO orders VALUES %s", strings.Join(vals, ",")), args...)
 }
 m.Insert = time.Since(start)

 var dump string

 fmt.Printf("⏳ [Phase 3] Action: Concurrent Order Lookups (%d requests)...\n", ConcurrentTx)
 start = time.Now()
 var wg3 sync.WaitGroup
 wg3.Add(ConcurrentTx)
 for i := 0; i < ConcurrentTx; i++ {
  go func() {
   defer wg3.Done()
   var localDump string
   db.QueryRow("SELECT _id FROM orders WHERE _id = $1", fmt.Sprintf("order_%d", rand.Intn(TotalOrdersHistory))).Scan(&localDump)
  }()
 }
 wg3.Wait()
 m.OrderRead = time.Since(start) / time.Duration(ConcurrentTx)

 fmt.Printf("⏳ [Phase 4] Action: Mass Price Inflation Adjustments (%d updates)...\n", ConcurrentTx)
 start = time.Now()
 var wg4 sync.WaitGroup
 wg4.Add(ConcurrentTx)
 for i := 0; i < ConcurrentTx; i++ {
  go func() {
   defer wg4.Done()
   db.Exec("UPDATE orders SET total_bs = 9999999 WHERE _id = $1", fmt.Sprintf("order_%d", rand.Intn(TotalOrdersHistory)))
  }()
 }
 wg4.Wait()
 m.PriceAdjustment = time.Since(start) / time.Duration(ConcurrentTx)

 start = time.Now()
 db.Exec("DELETE FROM orders WHERE _id = 'order_50001'")
 m.DeleteOrder = time.Since(start)

 start = time.Now()
 r, _ := db.Query("SELECT _id FROM orders WHERE status = 'Processed' AND quantity BETWEEN 400 AND 500 ORDER BY usd_amount DESC LIMIT 5")
 for r.Next() {
  r.Scan(&dump)
 }
 r.Close()
 m.HighValueQuery = time.Since(start)

 start = time.Now()
 rc, _ := db.Query("SELECT _id FROM orders WHERE (status = 'Processed' AND quantity < 50) OR usd_amount > 9000 LIMIT 5")
 for rc.Next() {
  rc.Scan(&dump)
 }
 rc.Close()
 m.FraudDetection = time.Since(start)

 start = time.Now()
 rw, _ := db.Query("SELECT _id FROM orders WHERE customer_name LIKE 'Supermercado_777%' LIMIT 5")
 for rw.Next() {
  rw.Scan(&dump)
 }
 rw.Close()
 m.SearchCustomer = time.Since(start)

 start = time.Now()
 rp, _ := db.Query("SELECT _id FROM orders ORDER BY quantity DESC LIMIT 10 OFFSET 1000")
 for rp.Next() {
  rp.Scan(&dump)
 }
 rp.Close()
 m.TopOrdersRanking = time.Since(start)

 fmt.Println("⏳ [Phase 10] Report: Deep Pagination by USD Value (Offset 500k)...")
 start = time.Now()
 rdp, _ := db.Query("SELECT _id FROM orders ORDER BY usd_amount ASC LIMIT 5 OFFSET 500000")
 for rdp.Next() {
  rdp.Scan(&dump)
 }
 rdp.Close()
 m.DeepPagination = time.Since(start)

 start = time.Now()
 rdis, _ := db.Query("SELECT DISTINCT quantity FROM orders")
 for rdis.Next() {
  rdis.Scan(&dump)
 }
 rdis.Close()
 m.DistinctQuantities = time.Since(start)

 start = time.Now()
 rj, _ := db.Query("SELECT o._id FROM orders o LEFT JOIN branches b ON o.branch_id = b._id LEFT JOIN products p ON o.product_id = p._id WHERE o.customer_name LIKE 'Supermercado_999%' LIMIT 5")
 for rj.Next() {
  rj.Scan(&dump)
 }
 rj.Close()
 m.FullInvoiceJoin = time.Since(start)

 start = time.Now()
 rag, _ := db.Query("SELECT branch_id, COUNT(*) FROM orders GROUP BY branch_id")
 for rag.Next() {
  rag.Scan(&dump, &dump)
 }
 rag.Close()
 m.SalesByBranch = time.Since(start)

 start = time.Now()
 ram, _ := db.Query("SELECT branch_id, status, COUNT(*), SUM(usd_amount) FROM orders GROUP BY branch_id, status")
 for ram.Next() {
  ram.Scan(&dump, &dump, &dump, &dump)
 }
 ram.Close()
 m.GlobalRevenue = time.Since(start)

 start = time.Now()
 rfm, _ := db.Query("SELECT _id FROM orders WHERE status = 'Pending' LIMIT 100")
 var ids []string
 for rfm.Next() {
  var id string
  rfm.Scan(&id)
  ids = append(ids, "'"+id+"'")
 }
 rfm.Close()
 if len(ids) > 0 {
  db.Exec(fmt.Sprintf("UPDATE orders SET status = 'Shipped' WHERE _id IN (%s)", strings.Join(ids, ",")))
 }
 m.MassDispatch = time.Since(start)

 start = time.Now()
 db.Exec("UPDATE orders SET quantity = 5000 WHERE _id IN ('order_10', 'order_20')")
 m.BulkAdjustment = time.Since(start)

 start = time.Now()
 botKeys := make([]string, 10000)
 for i := 0; i < 10000; i++ {
  botKeys[i] = fmt.Sprintf("'order_%d'", TotalOrdersHistory-i-1)
 }
 db.Exec(fmt.Sprintf("DELETE FROM orders WHERE _id IN (%s)", strings.Join(botKeys, ",")))
 m.PurgeOldOrders = time.Since(start)

 start = time.Now()
 tx, _ := db.Begin()
 tx.Exec("UPDATE orders SET usd_amount = 0 WHERE _id = 'order_10'")
 tx.Rollback()
 m.PaymentACIDTx = time.Since(start)

 start = time.Now()
 var wg sync.WaitGroup
 wg.Add(ConcurrentTx)
 for i := 0; i < ConcurrentTx; i++ {
  go func(txID int) {
   defer wg.Done()
   targetOrder := fmt.Sprintf("order_%d", rand.Intn(TotalOrdersHistory-10000))
   if txID%2 == 0 {
    db.QueryRow("SELECT _id FROM orders WHERE _id = $1", targetOrder).Scan(&dump)
   } else {
    db.Exec("UPDATE orders SET status = 'Processed' WHERE _id = $1", targetOrder)
   }
  }(i)
 }
 wg.Wait()
 m.QuincenaRush = time.Since(start)

 start = time.Now()
 db.Exec(fmt.Sprintf("DROP DATABASE %s", dbName))
 m.Cleanup = time.Since(start)

 return m
}

// ==========================================
// 3-COLUMN RESULTS PRINTER
// ==========================================
func printComparison(mt, maria, pg Metrics) {
 fmt.Println("\n================================================================================================================================")
 fmt.Printf(" 📈 FINAL PERFORMANCE RESULTS: LunaDB vs MARIADB vs POSTGRESQL (ENTERPRISE ERP: %dK DB, %dk CCU) 📈 \n", TotalOrdersHistory/1000, ConcurrentTx/1000)
 fmt.Println("================================================================================================================================")

 w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.AlignRight|tabwriter.Debug)

 fmt.Fprintln(w, "Business Process / Metric\tLunaDB\tMariaDB\tPostgreSQL\tAbsolute Winner\t")
 fmt.Fprintln(w, "-------------------------\t------------\t-------\t----------\t---------------\t")

 compare := func(name string, t1, t2, t3 time.Duration) {
  winner := "LunaDB 🐺"
  minTime := t1

  if t2 < minTime {
   winner = "MariaDB 🐬"
   minTime = t2
  }
  if t3 < minTime {
   winner = "PostgreSQL 🐘"
  }

  fmt.Fprintf(w, "%s\t%v\t%v\t%v\t%s\t\n", name, t1, t2, t3, winner)
 }

 compare("0. Boot: ERP System Conn", mt.Connection, maria.Connection, pg.Connection)
 compare("1. Init: Load Schema/Indexes", mt.Schema, maria.Schema, pg.Schema)
 compare("2. Data Gen: Hist. Orders", mt.Insert, maria.Insert, pg.Insert)
 compare("3. Action: Concurrent Lookup", mt.OrderRead, maria.OrderRead, pg.OrderRead)
 compare("4. Action: Price Adjustments", mt.PriceAdjustment, maria.PriceAdjustment, pg.PriceAdjustment)
 compare("5. Action: Cancel Order (Del)", mt.DeleteOrder, maria.DeleteOrder, pg.DeleteOrder)
 compare("6. Report: High Value Orders", mt.HighValueQuery, maria.HighValueQuery, pg.HighValueQuery)
 compare("7. Audit: Fraud Detection", mt.FraudDetection, maria.FraudDetection, pg.FraudDetection)
 compare("8. Support: Search Customer", mt.SearchCustomer, maria.SearchCustomer, pg.SearchCustomer)
 compare("9. Report: Top Largest Orders", mt.TopOrdersRanking, maria.TopOrdersRanking, pg.TopOrdersRanking)
 compare("10. Report: Deep Pagin. (USD)", mt.DeepPagination, maria.DeepPagination, pg.DeepPagination)
 compare("11. BI: Distinct Quantities", mt.DistinctQuantities, maria.DistinctQuantities, pg.DistinctQuantities)
 compare("12. ERP: Full Invoice (Joins)", mt.FullInvoiceJoin, maria.FullInvoiceJoin, pg.FullInvoiceJoin)
 compare("13. BI: Orders by Branch", mt.SalesByBranch, maria.SalesByBranch, pg.SalesByBranch)
 compare("14. BI: USD Revenue by Branch", mt.GlobalRevenue, maria.GlobalRevenue, pg.GlobalRevenue)
 compare("15. Logistics: Mass Dispatch", mt.MassDispatch, maria.MassDispatch, pg.MassDispatch)
 compare("16. Admin: Bulk Qty Adjust", mt.BulkAdjustment, maria.BulkAdjustment, pg.BulkAdjustment)
 compare("17. Maint: Purge Old Orders", mt.PurgeOldOrders, maria.PurgeOldOrders, pg.PurgeOldOrders)
 compare("18. Finance: Payment ACID Tx", mt.PaymentACIDTx, maria.PaymentACIDTx, pg.PaymentACIDTx)
 compare("19. Stress: Quincena Rush", mt.QuincenaRush, maria.QuincenaRush, pg.QuincenaRush)

 w.Flush()
 fmt.Println("================================================================================================================================")
 fmt.Println("Note: Simulating Corporate ERP Peak. Quincena Rush mixes high concurrency reads & status updates.")
}

```
