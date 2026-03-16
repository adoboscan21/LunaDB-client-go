# 🐺 LunaDB Go Client SDK

The official Go client library for **LunaDB**. This SDK provides a thread-safe, connection-pooled, and highly optimized interface to interact with your LunaDB server. It handles TLS encryption, connection multiplexing, and automatic Go-struct-to-BSON serialization seamlessly.

## 📦 Installation

```bash
go get github.com/adoboscan21/lunadb-client-go
````

## 🚀 Quick Start

```Go
package main

import (
 "fmt"
 "log"

 lunadb "github.com/adoboscan21/lunadb-client-go"
)

func main() {
 // 1. Initialize the client with connection pooling
 client := lunadb.NewClient(lunadb.ClientOptions{
  Host:               "localhost",
  Port:               5876,
  Username:           "admin",
  Password:           "adminpass",
  InsecureSkipVerify: true, // Use false in production with valid certs
  PoolSize:           10,   // Number of concurrent connections
 })

 // 2. Connect to the server
 if err := client.Connect(); err != nil {
  log.Fatalf("Failed to connect: %v", err)
 }
 defer client.Close()

 fmt.Println("Successfully connected to LunaDB!")
}
```

---

## 🗂️ Collection Management

Collections are physical buckets on the disk. You must create them before inserting data.

```Go
// Create a collection
resp, err := client.CollectionCreate("users")

// List available collections
collections, err := client.CollectionList()

// Delete a collection
resp, err = client.CollectionDelete("old_data")
```

---

## 📄 CRUD Operations (Single Item)

LunaDB uses BSON under the hood. You can pass standard Go `map[string]any` or custom `structs` (with `bson` tags) and the client will serialize them automatically.

### Insert / Overwrite (`Set`)

```Go
user := map[string]any{
    "name":  "Alice",
    "email": "alice@example.com",
    "age":   28,
}
// Signature: CollectionItemSet(collection, key, data)
resp, err := client.CollectionItemSet("users", "user_1", user)
```

### Retrieve (`Get`)

```Go
res, err := client.CollectionItemGet("users", "user_1")
if err == nil && res.Found() {
    var retrievedUser map[string]any
    res.Value(&retrievedUser) // Unmarshal BSON into Go struct/map
    fmt.Println("Found user:", retrievedUser["name"])
}
```

### Partial Update (`Update`)

Performs a Zero-Copy BSON patch on the server.

```Go
patch := map[string]any{"age": 29}
resp, err := client.CollectionItemUpdate("users", "user_1", patch)
```

### Delete (`Delete`)

```Go
resp, err := client.CollectionItemDelete("users", "user_1")
```

---

## ⚡ Batch Operations (High-Throughput)

For inserting or updating thousands of records, always use batch operations. They are sent in a single network request and committed as a single ACID transaction on the disk.

```Go
// 1. Bulk Insert
users := []any{
    map[string]any{"_id": "u2", "name": "Bob"},
    map[string]any{"_id": "u3", "name": "Charlie"},
}
client.CollectionItemSetMany("users", users)

// 2. Bulk Update (Requires "_id" and "patch" keys)
updates := []any{
    map[string]any{"_id": "u2", "patch": map[string]any{"role": "admin"}},
    map[string]any{"_id": "u3", "patch": map[string]any{"role": "user"}},
}
client.CollectionItemUpdateMany("users", updates)

// 3. Bulk Delete
keysToDelete := []string{"u2", "u3"}
client.CollectionItemDeleteMany("users", keysToDelete)
```

---

## 🔍 Querying & Indexing

To get the most out of LunaDB's speed, create B-Tree indexes on fields you query frequently.

```Go
// Create an index
client.CollectionIndexCreate("users", "age")
```

### The `Query` Builder

LunaDB's query engine is extremely powerful. Use the `Query` struct to define filters, sorting, and pagination.

```Go
limit := 10
q := lunadb.Query{
    Filter: map[string]any{
        "and": []any{
            map[string]any{"field": "age", "op": ">=", "value": 18},
            map[string]any{"field": "role", "op": "=", "value": "admin"},
        },
    },
    OrderBy: []lunadb.OrderByClause{
        {Field: "age", Direction: "desc"},
    },
    Limit:  &limit,
    Offset: 0,
}

resp, err := client.CollectionQuery("users", q)
if resp.OK() {
    var results []map[string]any
    // Use UnmarshalResults for Query responses!
    resp.UnmarshalResults(&results) 
    fmt.Printf("Found %d users\n", len(results))
}
```

### Server-Side Aggregations

Compute sums, counts, and averages directly on the database server.

```Go
qAgg := lunadb.Query{
    GroupBy: []string{"role"},
    Aggregations: map[string]any{
        "total_users": map[string]any{"func": "count", "field": "*"},
        "average_age": map[string]any{"func": "avg", "field": "age"},
    },
}
resp, _ := client.CollectionQuery("users", qAgg)
```

---

## 🎯 Massive Server-Side Mutations

Update or delete thousands of records instantly using a query filter. The data never travels over the network; the engine handles it internally.

### Update Where

Update all `pending` orders to `shipped`:

```Go
filter := lunadb.Query{
    Filter: map[string]any{"field": "status", "op": "=", "value": "pending"},
}
patch := map[string]any{"status": "shipped"}

resp, err := client.CollectionUpdateWhere("orders", filter, patch)
```

### Delete Where

Purge all cancelled orders:

```Go
filter := lunadb.Query{
    Filter: map[string]any{"field": "status", "op": "=", "value": "cancelled"},
}
resp, err := client.CollectionDeleteWhere("orders", filter)
```

---

## 📦 Stateful ACID Transactions

LunaDB supports multi-statement ACID transactions tied to a specific network connection. If anything fails, you can roll back all changes.

```Go
// 1. Begin the transaction (hijacks a connection from the pool)
tx, err := client.Begin()
if err != nil {
    log.Fatal(err)
}

// 2. Execute mutations using the `tx` object
_, err1 := tx.CollectionItemSet("accounts", "acc_1", map[string]any{"balance": 900})
_, err2 := tx.CollectionItemSet("accounts", "acc_2", map[string]any{"balance": 1100})

// 3. Evaluate and Commit or Rollback
if err1 != nil || err2 != nil {
    tx.Rollback()
    fmt.Println("Transaction aborted!")
} else {
    tx.Commit()
    fmt.Println("Transaction successful!")
}
```

---

## 🛑 Error Handling

Always check the `error` returned by the function (for network/protocol issues) and the `resp.OK()` method (for database logical errors).

```Go
resp, err := client.CollectionItemGet("users", "ghost")
if err != nil {
    log.Println("Network/Client error:", err)
    return
}

if !resp.OK() {
    // Will print something like: NOT_FOUND: Key 'ghost' does not exist.
    fmt.Printf("Database error: %s: %s\n", resp.Status, resp.Message)
}
```
