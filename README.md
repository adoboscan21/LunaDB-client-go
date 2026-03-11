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
 "text/tabwriter"
 "time"

 mtclient "github.com/adoboscan21/lunadb-client-go"
 _ "github.com/go-sql-driver/mysql" // MariaDB/MySQL Driver
 _ "github.com/lib/pq"              // PostgreSQL Driver
)

// Metrics stores the execution times for each phase
type Metrics struct {
 EngineName           string
 Connection           time.Duration
 Schema               time.Duration
 Insert               time.Duration
 LoginRead            time.Duration
 LootUpdate           time.Duration
 DeleteChar           time.Duration
 EventQuery           time.Duration
 AntiCheat            time.Duration
 SearchPlayer         time.Duration
 ShortLeaderboard     time.Duration
 DeepLeaderboard      time.Duration
 DistinctLevels       time.Duration
 ViewProfile          time.Duration
 SimpleCensus         time.Duration
 GlobalEconomy        time.Duration
 ServerMaintenance    time.Duration
 ExpEventUpdate       time.Duration
 BanWave              time.Duration
 SafeTradeTx          time.Duration
 AdenSiegeConcurrency time.Duration
 Cleanup              time.Duration
}

// printJSON is a helper to beautifully format output data
func printJSON(data any) {
 pretty, err := json.MarshalIndent(data, "      ", "  ")
 if err != nil {
  fmt.Println("      [Error formatting JSON]")
  return
 }
 fmt.Printf("      %s\n", string(pretty))
}

func main() {
 fmt.Println("=========================================================================================")
 fmt.Println(" 🐉 LINEAGE II MMORPG SIMULATOR: LunaDB vs MARIADB vs POSTGRESQL (1M CHARS) 🐉 ")
 fmt.Println("=========================================================================================")

 mtMetrics := runMemoryTools()
 mariaMetrics := runMariaDB()
 pgMetrics := runPostgreSQL()

 printComparison(mtMetrics, mariaMetrics, pgMetrics)
}

// ==========================================
// ENGINE 1: LunaDB
// ==========================================
func runMemoryTools() Metrics {
 fmt.Println("\n>>> [1/3] BOOTING GAME SERVER WITH LunaDB <<<")
 var m Metrics
 m.EngineName = "LunaDB"

 // --- Phase 0: Connection ---
 fmt.Println("⏳ [Phase 0] Boot: Connecting to Login Server (Pool: 100)...")
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
 fmt.Printf("✅ Login Server Online in %v\n", m.Connection)

 colChars := fmt.Sprintf("l2_chars_%d", time.Now().UnixNano())
 colCastles := fmt.Sprintf("l2_castles_%d", time.Now().UnixNano())
 colClasses := fmt.Sprintf("l2_classes_%d", time.Now().UnixNano())

 // --- Phase 1: Schema and Indexes ---
 fmt.Printf("⏳ [Phase 1] Init: Creating World (Aden, Elmore) and Indexes...\n")
 start = time.Now()
 client.CollectionCreate(colChars)
 client.CollectionCreate(colCastles)
 client.CollectionCreate(colClasses)

 indexes := []string{"level", "adena", "castle_id", "class_id", "status", "status,level", "name"}
 for _, idx := range indexes {
  client.CollectionIndexCreate(colChars, idx)
 }
 m.Schema = time.Since(start)
 fmt.Printf("✅ World initialized in %v\n", m.Schema)

 // --- Phase 2: Massive Insertion ---
 fmt.Println("⏳ [Phase 2] Spawn: Generating 10 Castles, 20 Classes, and 1,000,000 Characters...")
 start = time.Now()

 castles := []string{"Aden", "Giran", "Goddard", "Oren", "Gludio", "Dion", "Innadril", "Rune", "Schuttgart"}
 castlesData := make([]any, len(castles))
 for i, c := range castles {
  castlesData[i] = map[string]any{"_id": fmt.Sprintf("castle_%d", i), "name": c, "tax_rate": float64(15.0)}
 }
 client.CollectionItemSetMany(colCastles, castlesData)

 classes := []string{"Gladiator", "Warlord", "Paladin", "Dark Avenger", "Treasure Hunter", "Hawkeye", "Sorcerer", "Necromancer", "Warlock", "Bishop", "Prophet", "Shillien Knight", "Bladedancer", "Abyss Walker", "Phantom Ranger", "Spellhowler", "Shillien Elder", "Overlord", "Warcryer", "Destroyer"}
 classesData := make([]any, len(classes))
 for i, c := range classes {
  classesData[i] = map[string]any{"_id": fmt.Sprintf("class_%d", i), "title": c}
 }
 client.CollectionItemSetMany(colClasses, classesData)

 totalChars := 1000000
 batchSize := 100000
 statuses := []string{"Online", "Offline", "PeaceZone", "Chaotic"}

 for i := 0; i < totalChars; i += batchSize {
  batch := make([]any, 0, batchSize)
  for j := 0; j < batchSize; j++ {
   id := i + j
   batch = append(batch, map[string]any{
    "_id":       fmt.Sprintf("char_%d", id),
    "name":      fmt.Sprintf("Hero_%d", id),
    "level":     int32(rand.Intn(85) + 1),       // Max classic level 85
    "adena":     float64(rand.Intn(2000000000)), // Up to 2 Billion Adena
    "castle_id": fmt.Sprintf("castle_%d", rand.Intn(len(castles))),
    "class_id":  fmt.Sprintf("class_%d", rand.Intn(len(classes))),
    "status":    statuses[rand.Intn(len(statuses))],
   })
  }
  client.CollectionItemSetMany(colChars, batch)
 }
 m.Insert = time.Since(start)
 fmt.Printf("✅ 1 Million characters spawned in %v\n", m.Insert)

 // --- Phase 3: Point Read ---
 fmt.Println("⏳ [Phase 3] Action: Character Login (Point Read)...")
 start = time.Now()
 for i := 0; i < 100; i++ {
  client.CollectionItemGet(colChars, "char_50000")
 }
 m.LoginRead = time.Since(start) / 100
 fmt.Printf("✅ Character logged in %v (avg)\n", m.LoginRead)

 // --- Phase 4: Point Update ---
 fmt.Println("⏳ [Phase 4] Action: Loot Adena (Point Update)...")
 start = time.Now()
 for i := 0; i < 100; i++ {
  client.CollectionItemUpdate(colChars, "char_50000", map[string]any{"adena": float64(9999999)})
 }
 m.LootUpdate = time.Since(start) / 100
 fmt.Printf("✅ Adena updated in %v (avg)\n", m.LootUpdate)

 // --- Phase 5: Point Delete ---
 fmt.Println("⏳ [Phase 5] Action: PK Delete (Point Delete)...")
 start = time.Now()
 client.CollectionItemDelete(colChars, "char_50001")
 m.DeleteChar = time.Since(start)
 fmt.Printf("✅ Character deleted in %v\n", m.DeleteChar)

 // --- Phase 6: Deep Query ---
 fmt.Println("⏳ [Phase 6] Event: Seven Signs (lvl 70-85, Chaotic, Top Adena)...")
 start = time.Now()
 limit5 := 5
 qDeep := mtclient.Query{
  Filter: map[string]any{
   "and": []any{
    map[string]any{"field": "status", "op": "=", "value": "Chaotic"},
    map[string]any{"field": "level", "op": "between", "value": []any{int32(70), int32(85)}},
   },
  },
  OrderBy: []any{map[string]any{"field": "adena", "direction": "desc"}},
  Limit:   &limit5,
 }
 client.CollectionQuery(colChars, qDeep)
 m.EventQuery = time.Since(start)
 fmt.Printf("✅ Event query completed in %v\n", m.EventQuery)

 // --- Phase 7: Complex Filter (Short-Circuit) ---
 fmt.Println("⏳ [Phase 7] Admin: Anti-Bot Scan (Lvl < 20 Chaotic OR Adena > 1.9B)...")
 start = time.Now()
 qComplex := mtclient.Query{
  Filter: map[string]any{
   "or": []any{
    map[string]any{
     "and": []any{
      map[string]any{"field": "status", "op": "=", "value": "Chaotic"},
      map[string]any{"field": "level", "op": "<", "value": int32(20)},
     },
    },
    map[string]any{"field": "adena", "op": ">", "value": float64(1900000000)},
   },
  },
  Limit: &limit5,
 }
 client.CollectionQuery(colChars, qComplex)
 m.AntiCheat = time.Since(start)
 fmt.Printf("✅ Anti-Bot scan completed in %v\n", m.AntiCheat)

 // --- Phase 8: Wildcard Search ---
 fmt.Println("⏳ [Phase 8] UI: Search Clan Member (name LIKE 'Hero_777%')...")
 start = time.Now()
 qWildcard := mtclient.Query{
  Filter: map[string]any{"field": "name", "op": "like", "value": "Hero_777%"},
  Limit:  &limit5,
 }
 client.CollectionQuery(colChars, qWildcard)
 m.SearchPlayer = time.Since(start)
 fmt.Printf("✅ Search completed in %v\n", m.SearchPlayer)

 // --- Phase 9: Short Pagination ---
 fmt.Println("⏳ [Phase 9] UI: Top 10 PvP Players (Offset 1000)...")
 start = time.Now()
 limit10 := 10
 qPag := mtclient.Query{
  OrderBy: []any{map[string]any{"field": "level", "direction": "desc"}},
  Limit:   &limit10,
  Offset:  1000,
 }
 client.CollectionQuery(colChars, qPag)
 m.ShortLeaderboard = time.Since(start)
 fmt.Printf("✅ Leaderboard loaded in %v\n", m.ShortLeaderboard)

 // --- Phase 10: Deep Pagination ---
 fmt.Println("⏳ [Phase 10] UI: Deep Global Ranking (Offset 800k)...")
 start = time.Now()
 qDeepPag := mtclient.Query{
  OrderBy: []any{map[string]any{"field": "adena", "direction": "asc"}},
  Limit:   &limit5,
  Offset:  800000,
 }
 client.CollectionQuery(colChars, qDeepPag)
 m.DeepLeaderboard = time.Since(start)
 fmt.Printf("✅ Deep ranking completed in %v\n", m.DeepLeaderboard)

 // --- Phase 11: Distinct Values ---
 fmt.Println("⏳ [Phase 11] Stats: Level Distribution (Distinct 'level')...")
 start = time.Now()
 client.CollectionQuery(colChars, mtclient.Query{Distinct: "level"})
 m.DistinctLevels = time.Since(start)
 fmt.Printf("✅ Distinct levels calculated in %v\n", m.DistinctLevels)

 // --- Phase 12: Lookups (Multi-Joins) ---
 fmt.Println("⏳ [Phase 12] UI: View Profile (Multi-Join Castle/Class)...")
 start = time.Now()
 qJoin := mtclient.Query{
  Filter: map[string]any{"field": "name", "op": "like", "value": "Hero_999%"},
  Lookups: []any{
   map[string]any{"from": colCastles, "localField": "castle_id", "foreignField": "_id", "as": "castle_info"},
   map[string]any{"from": colClasses, "localField": "class_id", "foreignField": "_id", "as": "class_info"},
  },
  Limit: &limit5,
 }
 client.CollectionQuery(colChars, qJoin)
 m.ViewProfile = time.Since(start)
 fmt.Printf("✅ Profile loaded in %v\n", m.ViewProfile)

 // --- Phase 13: Simple Aggregation ---
 fmt.Println("⏳ [Phase 13] Stats: Castle Population (Simple Aggregation)...")
 start = time.Now()
 qAgg1 := mtclient.Query{
  GroupBy:      []string{"castle_id"},
  Aggregations: map[string]any{"total": map[string]any{"func": "count", "field": "*"}},
 }
 client.CollectionQuery(colChars, qAgg1)
 m.SimpleCensus = time.Since(start)
 fmt.Printf("✅ Population census completed in %v\n", m.SimpleCensus)

 // --- Phase 14: Multi-Level Aggregation ---
 fmt.Println("⏳ [Phase 14] Stats: Aden Global Economy (Multi-Aggregation)...")
 start = time.Now()
 qAgg2 := mtclient.Query{
  GroupBy: []string{"castle_id", "status"},
  Aggregations: map[string]any{
   "total":     map[string]any{"func": "count", "field": "*"},
   "sum_adena": map[string]any{"func": "sum", "field": "adena"},
  },
 }
 client.CollectionQuery(colChars, qAgg2)
 m.GlobalEconomy = time.Since(start)
 fmt.Printf("✅ Economy calculated in %v\n", m.GlobalEconomy)

 // --- Phase 15: Find & Modify (Two-Trip) ---
 fmt.Println("⏳ [Phase 15] Server: Maintenance (Move 1000 Online -> Offline)...")
 start = time.Now()
 limit1000 := 1000
 respPending, _ := client.CollectionQuery(colChars, mtclient.Query{
  Filter: map[string]any{"field": "status", "op": "=", "value": "Online"}, Limit: &limit1000, Projection: []string{"_id"},
 })
 var pendingUsers []map[string]any
 respPending.UnmarshalResults(&pendingUsers)
 bulkModify := make([]any, len(pendingUsers))
 for i, u := range pendingUsers {
  bulkModify[i] = map[string]any{"_id": u["_id"], "patch": map[string]any{"status": "Offline"}}
 }
 client.CollectionItemUpdateMany(colChars, bulkModify)
 m.ServerMaintenance = time.Since(start)
 fmt.Printf("✅ Maintenance completed in %v\n", m.ServerMaintenance)

 // --- Phase 16: Bulk Update ---
 fmt.Println("⏳ [Phase 16] Admin: Grant Max Level (Bulk Update)...")
 start = time.Now()
 client.CollectionItemUpdateMany(colChars, []any{
  map[string]any{"_id": "char_10", "patch": map[string]any{"level": int32(85)}},
  map[string]any{"_id": "char_20", "patch": map[string]any{"level": int32(85)}},
 })
 m.ExpEventUpdate = time.Since(start)
 fmt.Printf("✅ EXP Event applied in %v\n", m.ExpEventUpdate)

 // --- Phase 17: Bulk Delete ---
 fmt.Println("⏳ [Phase 17] Admin: Bot Ban Wave (Bulk Delete)...")
 start = time.Now()
 client.CollectionItemDeleteMany(colChars, []string{"char_60", "char_70"})
 m.BanWave = time.Since(start)
 fmt.Printf("✅ Ban wave completed in %v\n", m.BanWave)

 // --- Phase 18: ACID Transaction ---
 fmt.Println("⏳ [Phase 18] Trade: Safe Exchange (ACID Transaction)...")
 start = time.Now()
 tx, _ := client.Begin()
 tx.CollectionItemUpdate(colChars, "char_10", map[string]any{"adena": 0})
 tx.Rollback() // Trade cancelled securely
 m.SafeTradeTx = time.Since(start)
 fmt.Printf("✅ Trade safely rolled back in %v\n", m.SafeTradeTx)

 // --- Phase 19: Concurrency ---
 fmt.Println("⏳ [Phase 19] Event: Aden Castle Siege (5000 concurrent reads)...")
 start = time.Now()
 var wg sync.WaitGroup
 wg.Add(5000)
 for i := 0; i < 5000; i++ {
  go func() {
   defer wg.Done()
   client.CollectionItemGet(colChars, fmt.Sprintf("char_%d", rand.Intn(100000)))
  }()
 }
 wg.Wait()
 m.AdenSiegeConcurrency = time.Since(start)
 fmt.Printf("✅ Castle Siege withstood without lag in %v\n", m.AdenSiegeConcurrency)

 // --- Phase 20: Cleanup ---
 fmt.Println("⏳ [Phase 20] Shutdown: World Wipe (Cleanup)...")
 start = time.Now()
 client.CollectionDelete(colChars)
 client.CollectionDelete(colCastles)
 client.CollectionDelete(colClasses)
 m.Cleanup = time.Since(start)
 fmt.Printf("✅ World wiped in %v\n", m.Cleanup)

 return m
}

// ==========================================
// ENGINE 2: MARIADB
// ==========================================
func runMariaDB() Metrics {
 fmt.Println("\n>>> [2/3] BOOTING GAME SERVER WITH MARIADB <<<")
 var m Metrics
 m.EngineName = "MariaDB"

 start := time.Now()
 dbInit, _ := sql.Open("mysql", "root:12345678@tcp(127.0.0.1:3306)/?multiStatements=true")
 dbInit.Ping()
 m.Connection = time.Since(start)

 dbName := fmt.Sprintf("l2_db_%d", time.Now().UnixNano())
 start = time.Now()
 dbInit.Exec(fmt.Sprintf("CREATE DATABASE %s;", dbName))
 dbInit.Close()
 db, _ := sql.Open("mysql", fmt.Sprintf("root:12345678@tcp(127.0.0.1:3306)/%s?multiStatements=true", dbName))
 defer db.Close()
 db.SetMaxOpenConns(100)

 createTables := `
  CREATE TABLE castles (_id VARCHAR(50) PRIMARY KEY, name VARCHAR(100), tax_rate DOUBLE);
  CREATE TABLE classes (_id VARCHAR(50) PRIMARY KEY, title VARCHAR(100));
  CREATE TABLE characters (
   _id VARCHAR(50) PRIMARY KEY, name VARCHAR(100), level INT, adena DOUBLE, castle_id VARCHAR(50), class_id VARCHAR(50), status VARCHAR(20),
   INDEX idx_lvl (level), INDEX idx_adena (adena), INDEX idx_castle (castle_id), INDEX idx_class (class_id), INDEX idx_status (status), INDEX idx_name(name)
  );
 `
 db.Exec(createTables)
 m.Schema = time.Since(start)

 start = time.Now()
 for i := 0; i < 9; i++ {
  db.Exec("INSERT INTO castles VALUES (?, ?, ?)", fmt.Sprintf("castle_%d", i), "C", 15.0)
 }
 for i := 0; i < 20; i++ {
  db.Exec("INSERT INTO classes VALUES (?, ?)", fmt.Sprintf("class_%d", i), "Cls")
 }
 statuses := []string{"Online", "Offline", "PeaceZone", "Chaotic"}
 for i := 0; i < 1000000; i += 5000 {
  vals, args := []string{}, []any{}
  for j := 0; j < 5000; j++ {
   id := i + j
   vals = append(vals, "(?, ?, ?, ?, ?, ?, ?)")
   args = append(args, fmt.Sprintf("char_%d", id), fmt.Sprintf("Hero_%d", id), rand.Intn(85)+1, float64(rand.Intn(2000000000)), fmt.Sprintf("castle_%d", rand.Intn(9)), fmt.Sprintf("class_%d", rand.Intn(20)), statuses[rand.Intn(4)])
  }
  db.Exec(fmt.Sprintf("INSERT INTO characters VALUES %s", strings.Join(vals, ",")), args...)
 }
 m.Insert = time.Since(start)

 var dump string
 start = time.Now()
 for i := 0; i < 100; i++ {
  db.QueryRow("SELECT _id FROM characters WHERE _id = 'char_50000'").Scan(&dump)
 }
 m.LoginRead = time.Since(start) / 100

 start = time.Now()
 for i := 0; i < 100; i++ {
  db.Exec("UPDATE characters SET adena = 9999999 WHERE _id = 'char_50000'")
 }
 m.LootUpdate = time.Since(start) / 100

 start = time.Now()
 db.Exec("DELETE FROM characters WHERE _id = 'char_50001'")
 m.DeleteChar = time.Since(start)

 start = time.Now()
 r, _ := db.Query("SELECT _id FROM characters WHERE status = 'Chaotic' AND level BETWEEN 70 AND 85 ORDER BY adena DESC LIMIT 5")
 for r.Next() {
  r.Scan(&dump)
 }
 r.Close()
 m.EventQuery = time.Since(start)

 start = time.Now()
 rc, _ := db.Query("SELECT _id FROM characters WHERE (status = 'Chaotic' AND level < 20) OR adena > 1900000000 LIMIT 5")
 for rc.Next() {
  rc.Scan(&dump)
 }
 rc.Close()
 m.AntiCheat = time.Since(start)

 start = time.Now()
 rw, _ := db.Query("SELECT _id FROM characters WHERE name LIKE 'Hero_777%' LIMIT 5")
 for rw.Next() {
  rw.Scan(&dump)
 }
 rw.Close()
 m.SearchPlayer = time.Since(start)

 start = time.Now()
 rp, _ := db.Query("SELECT _id FROM characters ORDER BY level DESC LIMIT 10 OFFSET 1000")
 for rp.Next() {
  rp.Scan(&dump)
 }
 rp.Close()
 m.ShortLeaderboard = time.Since(start)

 start = time.Now()
 rdp, _ := db.Query("SELECT _id FROM characters ORDER BY adena ASC LIMIT 5 OFFSET 800000")
 for rdp.Next() {
  rdp.Scan(&dump)
 }
 rdp.Close()
 m.DeepLeaderboard = time.Since(start)

 start = time.Now()
 rdis, _ := db.Query("SELECT DISTINCT level FROM characters")
 for rdis.Next() {
  rdis.Scan(&dump)
 }
 rdis.Close()
 m.DistinctLevels = time.Since(start)

 start = time.Now()
 rj, _ := db.Query("SELECT c._id FROM characters c LEFT JOIN castles m ON c.castle_id = m._id LEFT JOIN classes cls ON c.class_id = cls._id WHERE c.name LIKE 'Hero_999%' LIMIT 5")
 for rj.Next() {
  rj.Scan(&dump)
 }
 rj.Close()
 m.ViewProfile = time.Since(start)

 start = time.Now()
 rag, _ := db.Query("SELECT castle_id, COUNT(*) FROM characters GROUP BY castle_id")
 for rag.Next() {
  rag.Scan(&dump, &dump)
 }
 rag.Close()
 m.SimpleCensus = time.Since(start)

 start = time.Now()
 ram, _ := db.Query("SELECT castle_id, status, COUNT(*), SUM(adena) FROM characters GROUP BY castle_id, status")
 for ram.Next() {
  ram.Scan(&dump, &dump, &dump, &dump)
 }
 ram.Close()
 m.GlobalEconomy = time.Since(start)

 start = time.Now()
 rfm, _ := db.Query("SELECT _id FROM characters WHERE status = 'Online' LIMIT 1000")
 var ids []string
 for rfm.Next() {
  var id string
  rfm.Scan(&id)
  ids = append(ids, "'"+id+"'")
 }
 rfm.Close()
 if len(ids) > 0 {
  db.Exec(fmt.Sprintf("UPDATE characters SET status = 'Offline' WHERE _id IN (%s)", strings.Join(ids, ",")))
 }
 m.ServerMaintenance = time.Since(start)

 start = time.Now()
 db.Exec("UPDATE characters SET level = 85 WHERE _id IN ('char_10', 'char_20')")
 m.ExpEventUpdate = time.Since(start)

 start = time.Now()
 db.Exec("DELETE FROM characters WHERE _id IN ('char_60', 'char_70')")
 m.BanWave = time.Since(start)

 start = time.Now()
 tx, _ := db.Begin()
 tx.Exec("UPDATE characters SET adena = 0 WHERE _id = 'char_10'")
 tx.Rollback()
 m.SafeTradeTx = time.Since(start)

 start = time.Now()
 var wg sync.WaitGroup
 wg.Add(5000)
 for i := 0; i < 5000; i++ {
  go func() {
   defer wg.Done()
   db.QueryRow("SELECT _id FROM characters WHERE _id = ?", fmt.Sprintf("char_%d", rand.Intn(100000))).Scan(&dump)
  }()
 }
 wg.Wait()
 m.AdenSiegeConcurrency = time.Since(start)

 start = time.Now()
 db.Exec(fmt.Sprintf("DROP DATABASE %s", dbName))
 m.Cleanup = time.Since(start)

 return m
}

// ==========================================
// ENGINE 3: POSTGRESQL
// ==========================================
func runPostgreSQL() Metrics {
 fmt.Println("\n>>> [3/3] BOOTING GAME SERVER WITH POSTGRESQL <<<")
 var m Metrics
 m.EngineName = "PostgreSQL"

 start := time.Now()
 dbInit, err := sql.Open("postgres", "postgres://postgres:12345678@localhost:5432/postgres?sslmode=disable")
 if err != nil {
  log.Fatalf("❌ [PG FAIL] Connection error: %v", err)
 }
 dbInit.Ping()
 m.Connection = time.Since(start)

 dbName := fmt.Sprintf("l2_db_%d", time.Now().UnixNano())
 start = time.Now()
 dbInit.Exec(fmt.Sprintf("CREATE DATABASE %s;", dbName))
 dbInit.Close()

 db, _ := sql.Open("postgres", fmt.Sprintf("postgres://postgres:12345678@localhost:5432/%s?sslmode=disable", dbName))
 defer db.Close()
 db.SetMaxOpenConns(100)

 createTables := `
  CREATE TABLE castles (_id VARCHAR(50) PRIMARY KEY, name VARCHAR(100), tax_rate DOUBLE PRECISION);
  CREATE TABLE classes (_id VARCHAR(50) PRIMARY KEY, title VARCHAR(100));
  CREATE TABLE characters (
   _id VARCHAR(50) PRIMARY KEY, name VARCHAR(100), level INT, adena DOUBLE PRECISION, castle_id VARCHAR(50), class_id VARCHAR(50), status VARCHAR(20)
  );
  CREATE INDEX idx_lvl ON characters(level); CREATE INDEX idx_adena ON characters(adena);
  CREATE INDEX idx_castle ON characters(castle_id); CREATE INDEX idx_status ON characters(status); CREATE INDEX idx_name ON characters(name);
 `
 db.Exec(createTables)
 m.Schema = time.Since(start)

 start = time.Now()
 for i := 0; i < 9; i++ {
  db.Exec("INSERT INTO castles VALUES ($1, $2, $3)", fmt.Sprintf("castle_%d", i), "C", 15.0)
 }
 for i := 0; i < 20; i++ {
  db.Exec("INSERT INTO classes VALUES ($1, $2)", fmt.Sprintf("class_%d", i), "Cls")
 }
 statuses := []string{"Online", "Offline", "PeaceZone", "Chaotic"}

 // Postgres Insert (Fastest syntax for massive bulk inserts in standard PG)
 for i := 0; i < 1000000; i += 5000 {
  vals, args := []string{}, []any{}
  for j := 0; j < 5000; j++ {
   id := i + j
   vals = append(vals, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d)", j*7+1, j*7+2, j*7+3, j*7+4, j*7+5, j*7+6, j*7+7))
   args = append(args, fmt.Sprintf("char_%d", id), fmt.Sprintf("Hero_%d", id), rand.Intn(85)+1, float64(rand.Intn(2000000000)), fmt.Sprintf("castle_%d", rand.Intn(9)), fmt.Sprintf("class_%d", rand.Intn(20)), statuses[rand.Intn(4)])
  }
  db.Exec(fmt.Sprintf("INSERT INTO characters VALUES %s", strings.Join(vals, ",")), args...)
 }
 m.Insert = time.Since(start)

 var dump string
 start = time.Now()
 for i := 0; i < 100; i++ {
  db.QueryRow("SELECT _id FROM characters WHERE _id = 'char_50000'").Scan(&dump)
 }
 m.LoginRead = time.Since(start) / 100

 start = time.Now()
 for i := 0; i < 100; i++ {
  db.Exec("UPDATE characters SET adena = 9999999 WHERE _id = 'char_50000'")
 }
 m.LootUpdate = time.Since(start) / 100

 start = time.Now()
 db.Exec("DELETE FROM characters WHERE _id = 'char_50001'")
 m.DeleteChar = time.Since(start)

 start = time.Now()
 r, _ := db.Query("SELECT _id FROM characters WHERE status = 'Chaotic' AND level BETWEEN 70 AND 85 ORDER BY adena DESC LIMIT 5")
 for r.Next() {
  r.Scan(&dump)
 }
 r.Close()
 m.EventQuery = time.Since(start)

 start = time.Now()
 rc, _ := db.Query("SELECT _id FROM characters WHERE (status = 'Chaotic' AND level < 20) OR adena > 1900000000 LIMIT 5")
 for rc.Next() {
  rc.Scan(&dump)
 }
 rc.Close()
 m.AntiCheat = time.Since(start)

 start = time.Now()
 rw, _ := db.Query("SELECT _id FROM characters WHERE name LIKE 'Hero_777%' LIMIT 5")
 for rw.Next() {
  rw.Scan(&dump)
 }
 rw.Close()
 m.SearchPlayer = time.Since(start)

 start = time.Now()
 rp, _ := db.Query("SELECT _id FROM characters ORDER BY level DESC LIMIT 10 OFFSET 1000")
 for rp.Next() {
  rp.Scan(&dump)
 }
 rp.Close()
 m.ShortLeaderboard = time.Since(start)

 start = time.Now()
 rdp, _ := db.Query("SELECT _id FROM characters ORDER BY adena ASC LIMIT 5 OFFSET 800000")
 for rdp.Next() {
  rdp.Scan(&dump)
 }
 rdp.Close()
 m.DeepLeaderboard = time.Since(start)

 start = time.Now()
 rdis, _ := db.Query("SELECT DISTINCT level FROM characters")
 for rdis.Next() {
  rdis.Scan(&dump)
 }
 rdis.Close()
 m.DistinctLevels = time.Since(start)

 start = time.Now()
 rj, _ := db.Query("SELECT c._id FROM characters c LEFT JOIN castles m ON c.castle_id = m._id LEFT JOIN classes cls ON c.class_id = cls._id WHERE c.name LIKE 'Hero_999%' LIMIT 5")
 for rj.Next() {
  rj.Scan(&dump)
 }
 rj.Close()
 m.ViewProfile = time.Since(start)

 start = time.Now()
 rag, _ := db.Query("SELECT castle_id, COUNT(*) FROM characters GROUP BY castle_id")
 for rag.Next() {
  rag.Scan(&dump, &dump)
 }
 rag.Close()
 m.SimpleCensus = time.Since(start)

 start = time.Now()
 ram, _ := db.Query("SELECT castle_id, status, COUNT(*), SUM(adena) FROM characters GROUP BY castle_id, status")
 for ram.Next() {
  ram.Scan(&dump, &dump, &dump, &dump)
 }
 ram.Close()
 m.GlobalEconomy = time.Since(start)

 start = time.Now()
 rfm, _ := db.Query("SELECT _id FROM characters WHERE status = 'Online' LIMIT 1000")
 var ids []string
 for rfm.Next() {
  var id string
  rfm.Scan(&id)
  ids = append(ids, "'"+id+"'")
 }
 rfm.Close()
 if len(ids) > 0 {
  db.Exec(fmt.Sprintf("UPDATE characters SET status = 'Offline' WHERE _id IN (%s)", strings.Join(ids, ",")))
 }
 m.ServerMaintenance = time.Since(start)

 start = time.Now()
 db.Exec("UPDATE characters SET level = 85 WHERE _id IN ('char_10', 'char_20')")
 m.ExpEventUpdate = time.Since(start)

 start = time.Now()
 db.Exec("DELETE FROM characters WHERE _id IN ('char_60', 'char_70')")
 m.BanWave = time.Since(start)

 start = time.Now()
 tx, _ := db.Begin()
 tx.Exec("UPDATE characters SET adena = 0 WHERE _id = 'char_10'")
 tx.Rollback()
 m.SafeTradeTx = time.Since(start)

 start = time.Now()
 var wg sync.WaitGroup
 wg.Add(5000)
 for i := 0; i < 5000; i++ {
  go func() {
   defer wg.Done()
   db.QueryRow("SELECT _id FROM characters WHERE _id = $1", fmt.Sprintf("char_%d", rand.Intn(100000))).Scan(&dump)
  }()
 }
 wg.Wait()
 m.AdenSiegeConcurrency = time.Since(start)

 start = time.Now()
 // To safely drop a database in PostgreSQL, we must close connections to it and use the initial connection
 db.Close()
 dbInit, _ = sql.Open("postgres", "postgres://postgres:12345678@localhost:5432/postgres?sslmode=disable")
 dbInit.Exec(fmt.Sprintf("DROP DATABASE %s;", dbName))
 dbInit.Close()
 m.Cleanup = time.Since(start)

 return m
}

// ==========================================
// 3-COLUMN RESULTS PRINTER
// ==========================================
func printComparison(mt, maria, pg Metrics) {
 fmt.Println("\n================================================================================================================================")
 fmt.Println(" 🏆 FINAL PERFORMANCE RESULTS: LunaDB vs MARIADB vs POSTGRESQL (LINEAGE II SERVER: 1M CHARS) 🏆 ")
 fmt.Println("================================================================================================================================")

 w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', tabwriter.AlignRight|tabwriter.Debug)

 fmt.Fprintln(w, "Game Event / Metric\tLunaDB\tMariaDB\tPostgreSQL\tAbsolute Winner\t")
 fmt.Fprintln(w, "-------------------\t------------\t-------\t----------\t---------------\t")

 compare := func(name string, t1, t2, t3 time.Duration) {
  winner := "LunaDB 🚀"
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

 compare("0. Boot: Login Server Conn", mt.Connection, maria.Connection, pg.Connection)
 compare("1. Init: Load Maps/Zones", mt.Schema, maria.Schema, pg.Schema)
 compare("2. Spawn: Gen 1M Characters", mt.Insert, maria.Insert, pg.Insert)
 compare("3. Action: Login Char (Read)", mt.LoginRead, maria.LoginRead, pg.LoginRead)
 compare("4. Action: Loot Adena (Update)", mt.LootUpdate, maria.LootUpdate, pg.LootUpdate)
 compare("5. Action: PK Delete (Drop Char)", mt.DeleteChar, maria.DeleteChar, pg.DeleteChar)
 compare("6. Event: Seven Signs (Query)", mt.EventQuery, maria.EventQuery, pg.EventQuery)
 compare("7. Admin: Anti-Bot System", mt.AntiCheat, maria.AntiCheat, pg.AntiCheat)
 compare("8. UI: Search Clan Member", mt.SearchPlayer, maria.SearchPlayer, pg.SearchPlayer)
 compare("9. UI: Top 10 PvP Players", mt.ShortLeaderboard, maria.ShortLeaderboard, pg.ShortLeaderboard)
 compare("10. UI: Deep Global Ranking", mt.DeepLeaderboard, maria.DeepLeaderboard, pg.DeepLeaderboard)
 compare("11. Stats: Level Curve", mt.DistinctLevels, maria.DistinctLevels, pg.DistinctLevels)
 compare("12. UI: View Profile (Joins)", mt.ViewProfile, maria.ViewProfile, pg.ViewProfile)
 compare("13. Stats: Castle Population", mt.SimpleCensus, maria.SimpleCensus, pg.SimpleCensus)
 compare("14. Stats: Aden Economy", mt.GlobalEconomy, maria.GlobalEconomy, pg.GlobalEconomy)
 compare("15. Server: Mass Maintenance", mt.ServerMaintenance, maria.ServerMaintenance, pg.ServerMaintenance)
 compare("16. Admin: Grant Max EXP", mt.ExpEventUpdate, maria.ExpEventUpdate, pg.ExpEventUpdate)
 compare("17. Admin: Bot Ban Wave", mt.BanWave, maria.BanWave, pg.BanWave)
 compare("18. Trade: Safe Exchange", mt.SafeTradeTx, maria.SafeTradeTx, pg.SafeTradeTx)
 compare("19. Event: Aden Castle Siege", mt.AdenSiegeConcurrency, maria.AdenSiegeConcurrency, pg.AdenSiegeConcurrency)

 w.Flush()
 fmt.Println("================================================================================================================================")
 fmt.Println("Note: Simulating MMORPG production conditions. Execution times include network latency from Go client.")
}

```
