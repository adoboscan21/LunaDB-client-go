package client

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"syscall"

	"go.mongodb.org/mongo-driver/bson"
)

// --- Constantes del Protocolo ---
const (
	// Collection Management Commands
	cmdCollectionCreate      byte = 1
	cmdCollectionDelete      byte = 2
	cmdCollectionList        byte = 3
	cmdCollectionIndexCreate byte = 4
	cmdCollectionIndexDelete byte = 5
	cmdCollectionIndexList   byte = 6

	// Collection Item Commands
	cmdCollectionItemSet        byte = 7
	cmdCollectionItemSetMany    byte = 8
	cmdCollectionItemGet        byte = 9
	cmdCollectionItemDelete     byte = 10
	cmdCollectionQuery          byte = 12
	cmdCollectionItemDeleteMany byte = 13
	cmdCollectionItemUpdate     byte = 14
	cmdCollectionItemUpdateMany byte = 15
	cmdCollectionUpdateWhere    byte = 16
	cmdCollectionDeleteWhere    byte = 17

	// Authentication Commands
	cmdAuthenticate byte = 18

	// Transaction Commands
	cmdBegin    byte = 25
	cmdCommit   byte = 26
	cmdRollback byte = 27

	// Status Codes (Estos se mantienen igual)
	statusOK           = 1
	statusNotFound     = 2
	statusError        = 3
	statusBadCommand   = 4
	statusUnauthorized = 5
	statusBadRequest   = 6
)

var byteOrder = binary.LittleEndian

func getStatusString(status byte) string {
	switch status {
	case statusOK:
		return "OK"
	case statusNotFound:
		return "NOT_FOUND"
	case statusError:
		return "ERROR"
	case statusBadCommand:
		return "BAD_COMMAND"
	case statusUnauthorized:
		return "UNAUTHORIZED"
	case statusBadRequest:
		return "BAD_REQUEST"
	default:
		return "UNKNOWN_STATUS"
	}
}

// --- Estructuras de Respuesta ---

type CommandResponse struct {
	StatusCode byte
	Status     string
	Message    string
	RawData    []byte
}

func (r *CommandResponse) OK() bool {
	return r.StatusCode == statusOK
}

func (r *CommandResponse) BSON(v any) error {
	if len(r.RawData) == 0 {
		return fmt.Errorf("no data to decode")
	}
	return bson.Unmarshal(r.RawData, v)
}

func (r *CommandResponse) UnmarshalResults(v any) error {
	if len(r.RawData) == 0 {
		return fmt.Errorf("no data to decode")
	}
	rawDoc := bson.Raw(r.RawData)
	val, err := rawDoc.LookupErr("results")
	if err != nil {
		return fmt.Errorf("key 'results' not found in response: %w", err)
	}
	return val.Unmarshal(v)
}

type GetResult struct {
	*CommandResponse
}

func (r *GetResult) Found() bool {
	return r.OK()
}

func (r *GetResult) Value(v any) error {
	return r.BSON(v)
}

// --- Query Builder ---

// OrderByClause define el criterio de ordenamiento
type OrderByClause struct {
	Field     string `bson:"field"`
	Direction string `bson:"direction"` // "asc" o "desc"
}

// LookupClause define un join entre colecciones
type LookupClause struct {
	FromCollection string `bson:"from"`
	LocalField     string `bson:"localField"`
	ForeignField   string `bson:"foreignField"`
	As             string `bson:"as"`
}

// Query representa una consulta compleja a la base de datos
type Query struct {
	Filter       map[string]any  `bson:"filter,omitempty"`
	OrderBy      []OrderByClause `bson:"order_by,omitempty"` // <-- Tipado fuerte
	Limit        *int            `bson:"limit,omitempty"`
	Offset       int             `bson:"offset,omitempty"`
	Count        bool            `bson:"count,omitempty"`
	Aggregations map[string]any  `bson:"aggregations,omitempty"`
	GroupBy      []string        `bson:"group_by,omitempty"`
	Having       map[string]any  `bson:"having,omitempty"`
	Distinct     string          `bson:"distinct,omitempty"`
	Projection   []string        `bson:"projection,omitempty"`
	Lookups      []LookupClause  `bson:"lookups,omitempty"` // <-- Tipado fuerte
}

// --- Infraestructura del Connection Pool ---

type connWrapper struct {
	conn              net.Conn
	reader            *bufio.Reader
	writer            *bufio.Writer
	scratch           [8]byte
	authenticatedUser string
}

type Client struct {
	host           string
	port           int
	username       string
	password       string
	serverCertPath string
	insecure       bool

	poolSize int
	pool     chan *connWrapper
	mu       sync.Mutex
	isClosed bool
}

type ClientOptions struct {
	Host               string
	Port               int
	Username           string
	Password           string
	ServerCertPath     string
	InsecureSkipVerify bool
	PoolSize           int
}

func NewClient(opts ClientOptions) *Client {
	if opts.PoolSize <= 0 {
		opts.PoolSize = 5
	}
	return &Client{
		host:           opts.Host,
		port:           opts.Port,
		username:       opts.Username,
		password:       opts.Password,
		serverCertPath: opts.ServerCertPath,
		insecure:       opts.InsecureSkipVerify,
		poolSize:       opts.PoolSize,
		pool:           make(chan *connWrapper, opts.PoolSize),
	}
}

func (c *Client) Connect() error {
	var wg sync.WaitGroup
	errCh := make(chan error, c.poolSize)

	for i := 0; i < c.poolSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cw, err := c.dialAndAuth()
			if err != nil {
				errCh <- err
				return
			}
			c.pool <- cw
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			c.Close()
			return fmt.Errorf("fallo al inicializar el pool: %w", err)
		}
	}
	return nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isClosed {
		return nil
	}
	c.isClosed = true

	close(c.pool)
	for cw := range c.pool {
		if cw.conn != nil {
			cw.conn.Close()
		}
	}
	return nil
}

func (c *Client) IsAuthenticated() bool {
	return c.username != ""
}

func (c *Client) dialAndAuth() (*connWrapper, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: c.insecure,
	}

	if c.serverCertPath != "" {
		caCert, err := os.ReadFile(c.serverCertPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read server certificate: %w", err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig.RootCAs = caCertPool
	}

	addr := fmt.Sprintf("%s:%d", c.host, c.port)
	conn, err := tls.Dial("tcp", addr, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect via TLS: %w", err)
	}

	cw := &connWrapper{
		conn:   conn,
		reader: bufio.NewReaderSize(conn, 16*1024),
		writer: bufio.NewWriterSize(conn, 16*1024),
	}

	if c.username != "" && c.password != "" {
		if err := c.performAuthentication(cw); err != nil {
			cw.conn.Close()
			return nil, err
		}
	}
	return cw, nil
}

func (c *Client) performAuthentication(cw *connWrapper) error {
	err := c.sendAndFlush(cw, cmdAuthenticate, func(innerCw *connWrapper) error {
		if err := c.writeString(innerCw, c.username); err != nil {
			return err
		}
		return c.writeString(innerCw, c.password)
	})

	if err != nil {
		return fmt.Errorf("auth send failed: %w", err)
	}

	resp, err := c.readResponse(cw)
	if err != nil {
		return fmt.Errorf("auth read failed: %w", err)
	}

	if resp.StatusCode == statusOK {
		cw.authenticatedUser = c.username
		return nil
	}

	return fmt.Errorf("authentication failed: %s: %s", resp.Status, resp.Message)
}

func (c *Client) getConn() (*connWrapper, error) {
	cw, ok := <-c.pool
	if !ok {
		return nil, fmt.Errorf("client connection pool is closed")
	}
	return cw, nil
}

func (c *Client) putConn(cw *connWrapper) {
	c.pool <- cw
}

func (c *Client) replaceConn(cw *connWrapper) (*connWrapper, error) {
	if cw.conn != nil {
		cw.conn.Close()
	}
	return c.dialAndAuth()
}

// internalExecute maneja la lógica de envío/lectura sobre una conexión específica.
func (c *Client) internalExecute(cw *connWrapper, cmd byte, writerFunc func(cw *connWrapper) error, allowRetry bool) (*CommandResponse, error) {
	// 1. Escritura directa al buffer de alto rendimiento
	if err := cw.writer.WriteByte(cmd); err != nil {
		return nil, err
	}
	if writerFunc != nil {
		if err := writerFunc(cw); err != nil {
			return nil, err
		}
	}

	// Flush único: Aquí es donde estaba el cuello de botella
	if err := cw.writer.Flush(); err != nil {
		if !allowRetry {
			return nil, err
		}
		// ... lógica de reconexión simplificada ...
	}

	// 2. Lectura optimizada: Ya no bloqueamos más de lo necesario
	return c.readResponse(cw)
}

// execute es para comandos sueltos (toma una conexión y la devuelve).
func (c *Client) execute(cmd byte, writerFunc func(cw *connWrapper) error) (*CommandResponse, error) {
	// 1. Obtenemos una conexión del pool
	cw, err := c.getConn()
	if err != nil {
		return nil, err
	}

	// 2. Ejecutamos el comando
	resp, executeErr := c.internalExecute(cw, cmd, writerFunc, true)

	// 3. Evaluamos la salud de la conexión antes de devolverla
	if c.isNetworkError(executeErr) {
		// La conexión murió. Intentamos reemplazarla por una nueva.
		newCw, replaceErr := c.replaceConn(cw)
		if replaceErr == nil {
			// Éxito al reconectar, devolvemos la conexión sana al pool
			c.putConn(newCw)
		}
		// Si replaceErr falla, la conexión rota simplemente se descarta.
		// Podrías implementar lógica adicional para recuperar el tamaño del pool más adelante.
	} else {
		// Si no hubo error, o fue un error lógico de la BD (ej. clave no encontrada),
		// la conexión TCP sigue sana, así que la devolvemos normal.
		c.putConn(cw)
	}

	return resp, executeErr
}

// --- Optimizaciones de E/S ---

func (c *Client) writeString(cw *connWrapper, s string) error {
	l := uint32(len(s))
	byteOrder.PutUint32(cw.scratch[:4], l)
	if _, err := cw.writer.Write(cw.scratch[:4]); err != nil {
		return err
	}
	_, err := cw.writer.WriteString(s)
	return err
}

func (c *Client) writeBytes(cw *connWrapper, b []byte) error {
	l := uint32(len(b))
	byteOrder.PutUint32(cw.scratch[:4], l)
	if _, err := cw.writer.Write(cw.scratch[:4]); err != nil {
		return err
	}
	_, err := cw.writer.Write(b)
	return err
}

func (c *Client) writeUint32(cw *connWrapper, n uint32) error {
	byteOrder.PutUint32(cw.scratch[:4], n)
	_, err := cw.writer.Write(cw.scratch[:4])
	return err
}

func (c *Client) writeInt64(cw *connWrapper, n int64) error {
	byteOrder.PutUint64(cw.scratch[:8], uint64(n))
	_, err := cw.writer.Write(cw.scratch[:8])
	return err
}

func (c *Client) sendAndFlush(cw *connWrapper, cmd byte, writerFunc func(cw *connWrapper) error) error {
	if err := cw.writer.WriteByte(cmd); err != nil {
		return err
	}
	if writerFunc != nil {
		if err := writerFunc(cw); err != nil {
			return err
		}
	}
	return cw.writer.Flush()
}

func (c *Client) readResponse(cw *connWrapper) (*CommandResponse, error) {
	status, err := cw.reader.ReadByte()
	if err != nil {
		return nil, err
	}

	msg, err := c.readStringLocked(cw)
	if err != nil {
		return nil, err
	}

	data, err := c.readBytesLocked(cw)
	if err != nil {
		return nil, err
	}

	return &CommandResponse{
		StatusCode: status,
		Status:     getStatusString(status),
		Message:    msg,
		RawData:    data,
	}, nil
}

func (c *Client) readBytesLocked(cw *connWrapper) ([]byte, error) {
	if _, err := io.ReadFull(cw.reader, cw.scratch[:4]); err != nil {
		return nil, err
	}
	length := byteOrder.Uint32(cw.scratch[:4])

	if length == 0 {
		return nil, nil
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(cw.reader, data); err != nil {
		return nil, err
	}
	return data, nil
}

func (c *Client) readStringLocked(cw *connWrapper) (string, error) {
	data, err := c.readBytesLocked(cw)
	if err != nil {
		return "", err
	}
	if len(data) == 0 {
		return "", nil
	}
	return string(data), nil
}

func (c *Client) isNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	_, isNetErr := err.(net.Error)
	return isNetErr
}

// =========================================================================
// API TRANSACCIONAL (STATEFUL)
// =========================================================================

// Tx representa una transacción activa amarrada a una única conexión de red.
type Tx struct {
	client *Client
	cw     *connWrapper
	closed bool
}

// Begin inicia una transacción y secuestra la conexión.
func (c *Client) Begin() (*Tx, error) {
	cw, err := c.getConn()
	if err != nil {
		return nil, err
	}

	tx := &Tx{
		client: c,
		cw:     cw,
		closed: false,
	}

	resp, err := c.internalExecute(cw, cmdBegin, nil, true)
	if err != nil || !resp.OK() {
		// Si falla, devolvemos la conexión y retornamos error
		c.putConn(cw)
		return nil, fmt.Errorf("failed to begin transaction: %v", err)
	}

	return tx, nil
}

// execute asegura que la transacción sigue viva y usa la conexión secuestrada.
func (t *Tx) execute(cmd byte, writerFunc func(cw *connWrapper) error) (*CommandResponse, error) {
	if t.closed {
		return nil, errors.New("transaction is already closed")
	}

	// allowRetry = false. Si la red cae a mitad de la Tx, el server hace rollback.
	// Re-conectarnos arruinaría el estado. Debemos fallar explícitamente.
	resp, err := t.client.internalExecute(t.cw, cmd, writerFunc, false)
	if err != nil {
		t.Rollback() // Forzamos limpieza local
		return nil, err
	}
	return resp, nil
}

// Commit finaliza la transacción y devuelve la conexión al pool.
func (t *Tx) Commit() (*CommandResponse, error) {
	if t.closed {
		return nil, errors.New("transaction is already closed")
	}
	resp, err := t.execute(cmdCommit, nil)
	t.closed = true
	t.client.putConn(t.cw)
	return resp, err
}

// Rollback revierte la transacción y devuelve la conexión al pool.
func (t *Tx) Rollback() (*CommandResponse, error) {
	if t.closed {
		return nil, nil // Ya estaba cerrada
	}
	resp, err := t.execute(cmdRollback, nil)
	t.closed = true
	t.client.putConn(t.cw)
	return resp, err
}

// Métodos de mutación exclusivos de la transacción (Amarrados al Tx)
func (t *Tx) CollectionItemSet(collectionName, key string, value any) (*CommandResponse, error) {
	valueBytes, err := bson.Marshal(value)
	if err != nil {
		return nil, err
	}
	return t.execute(cmdCollectionItemSet, func(cw *connWrapper) error {
		if err := t.client.writeString(cw, collectionName); err != nil {
			return err
		}
		if err := t.client.writeString(cw, key); err != nil {
			return err
		}
		return t.client.writeBytes(cw, valueBytes)
	})
}

func (t *Tx) CollectionItemSetMany(collectionName string, items []any) (*CommandResponse, error) {
	itemsBytes, err := bson.Marshal(bson.M{"array": items})
	if err != nil {
		return nil, err
	}
	return t.execute(cmdCollectionItemSetMany, func(cw *connWrapper) error {
		if err := t.client.writeString(cw, collectionName); err != nil {
			return err
		}
		return t.client.writeBytes(cw, itemsBytes)
	})
}

func (t *Tx) CollectionItemUpdate(collectionName, key string, patchValue any) (*CommandResponse, error) {
	patchBytes, err := bson.Marshal(patchValue)
	if err != nil {
		return nil, err
	}
	return t.execute(cmdCollectionItemUpdate, func(cw *connWrapper) error {
		if err := t.client.writeString(cw, collectionName); err != nil {
			return err
		}
		if err := t.client.writeString(cw, key); err != nil {
			return err
		}
		return t.client.writeBytes(cw, patchBytes)
	})
}

func (t *Tx) CollectionItemUpdateMany(collectionName string, items []any) (*CommandResponse, error) {
	itemsBytes, err := bson.Marshal(bson.M{"array": items})
	if err != nil {
		return nil, err
	}
	return t.execute(cmdCollectionItemUpdateMany, func(cw *connWrapper) error {
		if err := t.client.writeString(cw, collectionName); err != nil {
			return err
		}
		return t.client.writeBytes(cw, itemsBytes)
	})
}

func (t *Tx) CollectionItemDelete(collectionName, key string) (*CommandResponse, error) {
	return t.execute(cmdCollectionItemDelete, func(cw *connWrapper) error {
		if err := t.client.writeString(cw, collectionName); err != nil {
			return err
		}
		return t.client.writeString(cw, key)
	})
}

func (t *Tx) CollectionItemDeleteMany(collectionName string, keys []string) (*CommandResponse, error) {
	return t.execute(cmdCollectionItemDeleteMany, func(cw *connWrapper) error {
		if err := t.client.writeString(cw, collectionName); err != nil {
			return err
		}
		if err := t.client.writeUint32(cw, uint32(len(keys))); err != nil {
			return err
		}
		for _, key := range keys {
			if err := t.client.writeString(cw, key); err != nil {
				return err
			}
		}
		return nil
	})
}

func (t *Tx) CollectionUpdateWhere(collectionName string, query Query, patch any) (*CommandResponse, error) {
	queryBytes, err := bson.Marshal(query)
	if err != nil {
		return nil, err
	}
	patchBytes, err := bson.Marshal(patch)
	if err != nil {
		return nil, err
	}

	return t.execute(cmdCollectionUpdateWhere, func(cw *connWrapper) error {
		if err := t.client.writeString(cw, collectionName); err != nil {
			return err
		}
		if err := t.client.writeBytes(cw, queryBytes); err != nil {
			return err
		}
		return t.client.writeBytes(cw, patchBytes)
	})
}

func (t *Tx) CollectionDeleteWhere(collectionName string, query Query) (*CommandResponse, error) {
	queryBytes, err := bson.Marshal(query)
	if err != nil {
		return nil, err
	}

	return t.execute(cmdCollectionDeleteWhere, func(cw *connWrapper) error {
		if err := t.client.writeString(cw, collectionName); err != nil {
			return err
		}
		return t.client.writeBytes(cw, queryBytes)
	})
}

// =========================================================================
// API PÚBLICA ESTÁNDAR (STATELESS)
// =========================================================================

func (c *Client) CollectionCreate(name string) (*CommandResponse, error) {
	return c.execute(cmdCollectionCreate, func(cw *connWrapper) error { return c.writeString(cw, name) })
}

func (c *Client) CollectionDelete(name string) (*CommandResponse, error) {
	return c.execute(cmdCollectionDelete, func(cw *connWrapper) error { return c.writeString(cw, name) })
}

func (c *Client) CollectionList() ([]string, error) {
	resp, err := c.execute(cmdCollectionList, nil)
	if err != nil {
		return nil, err
	}
	if !resp.OK() {
		return nil, fmt.Errorf("failed to list collections: %s: %s", resp.Status, resp.Message)
	}
	var wrapper struct {
		List []string `bson:"list"`
	}
	if err := resp.BSON(&wrapper); err != nil {
		return nil, fmt.Errorf("bson parse error: %w", err)
	}
	return wrapper.List, nil
}

func (c *Client) CollectionIndexCreate(collectionName, fieldName string) (*CommandResponse, error) {
	return c.execute(cmdCollectionIndexCreate, func(cw *connWrapper) error {
		if err := c.writeString(cw, collectionName); err != nil {
			return err
		}
		return c.writeString(cw, fieldName)
	})
}

func (c *Client) CollectionIndexDelete(collectionName, fieldName string) (*CommandResponse, error) {
	return c.execute(cmdCollectionIndexDelete, func(cw *connWrapper) error {
		if err := c.writeString(cw, collectionName); err != nil {
			return err
		}
		return c.writeString(cw, fieldName)
	})
}

func (c *Client) CollectionIndexList(collectionName string) ([]string, error) {
	resp, err := c.execute(cmdCollectionIndexList, func(cw *connWrapper) error { return c.writeString(cw, collectionName) })
	if err != nil {
		return nil, err
	}
	if !resp.OK() {
		return nil, fmt.Errorf("list indexes failed: %s: %s", resp.Status, resp.Message)
	}
	var wrapper struct {
		List []string `bson:"list"`
	}
	if err := resp.BSON(&wrapper); err != nil {
		return nil, fmt.Errorf("bson parse error: %w", err)
	}
	return wrapper.List, nil
}

func (c *Client) CollectionItemSet(collectionName, key string, value any) (*CommandResponse, error) {
	valueBytes, err := bson.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("bson marshal error: %w", err)
	}

	return c.execute(cmdCollectionItemSet, func(cw *connWrapper) error {
		if err := c.writeString(cw, collectionName); err != nil {
			return err
		}
		if err := c.writeString(cw, key); err != nil {
			return err
		}
		return c.writeBytes(cw, valueBytes)
	})
}

func (c *Client) CollectionItemSetMany(collectionName string, items []any) (*CommandResponse, error) {
	itemsBytes, err := bson.Marshal(bson.M{"array": items})
	if err != nil {
		return nil, fmt.Errorf("bson marshal error: %w", err)
	}
	return c.execute(cmdCollectionItemSetMany, func(cw *connWrapper) error {
		if err := c.writeString(cw, collectionName); err != nil {
			return err
		}
		return c.writeBytes(cw, itemsBytes)
	})
}

func (c *Client) CollectionItemUpdate(collectionName, key string, patchValue any) (*CommandResponse, error) {
	patchBytes, err := bson.Marshal(patchValue)
	if err != nil {
		return nil, fmt.Errorf("bson marshal error: %w", err)
	}
	return c.execute(cmdCollectionItemUpdate, func(cw *connWrapper) error {
		if err := c.writeString(cw, collectionName); err != nil {
			return err
		}
		if err := c.writeString(cw, key); err != nil {
			return err
		}
		return c.writeBytes(cw, patchBytes)
	})
}

func (c *Client) CollectionItemUpdateMany(collectionName string, items []any) (*CommandResponse, error) {
	itemsBytes, err := bson.Marshal(bson.M{"array": items})
	if err != nil {
		return nil, fmt.Errorf("bson marshal error: %w", err)
	}
	return c.execute(cmdCollectionItemUpdateMany, func(cw *connWrapper) error {
		if err := c.writeString(cw, collectionName); err != nil {
			return err
		}
		return c.writeBytes(cw, itemsBytes)
	})
}

func (c *Client) CollectionItemGet(collectionName, key string) (*GetResult, error) {
	resp, err := c.execute(cmdCollectionItemGet, func(cw *connWrapper) error {
		if err := c.writeString(cw, collectionName); err != nil {
			return err
		}
		return c.writeString(cw, key)
	})
	if err != nil {
		return nil, err
	}
	return &GetResult{CommandResponse: resp}, nil
}

func (c *Client) CollectionItemDelete(collectionName, key string) (*CommandResponse, error) {
	return c.execute(cmdCollectionItemDelete, func(cw *connWrapper) error {
		if err := c.writeString(cw, collectionName); err != nil {
			return err
		}
		return c.writeString(cw, key)
	})
}

func (c *Client) CollectionItemDeleteMany(collectionName string, keys []string) (*CommandResponse, error) {
	return c.execute(cmdCollectionItemDeleteMany, func(cw *connWrapper) error {
		if err := c.writeString(cw, collectionName); err != nil {
			return err
		}
		if err := c.writeUint32(cw, uint32(len(keys))); err != nil {
			return err
		}
		for _, key := range keys {
			if err := c.writeString(cw, key); err != nil {
				return err
			}
		}
		return nil
	})
}

func (c *Client) CollectionQuery(collectionName string, query Query) (*CommandResponse, error) {
	queryBytes, err := bson.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("bson marshal query error: %w", err)
	}

	return c.execute(cmdCollectionQuery, func(cw *connWrapper) error {
		if err := c.writeString(cw, collectionName); err != nil {
			return err
		}
		return c.writeBytes(cw, queryBytes)
	})
}

// CollectionUpdateWhere actualiza documentos que coincidan con el query especificado.
func (c *Client) CollectionUpdateWhere(collectionName string, query Query, patch any) (*CommandResponse, error) {
	queryBytes, err := bson.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("bson marshal query error: %w", err)
	}
	patchBytes, err := bson.Marshal(patch)
	if err != nil {
		return nil, fmt.Errorf("bson marshal patch error: %w", err)
	}

	return c.execute(cmdCollectionUpdateWhere, func(cw *connWrapper) error {
		if err := c.writeString(cw, collectionName); err != nil {
			return err
		}
		if err := c.writeBytes(cw, queryBytes); err != nil {
			return err
		}
		return c.writeBytes(cw, patchBytes)
	})
}

// CollectionDeleteWhere elimina documentos que coincidan con el query especificado.
func (c *Client) CollectionDeleteWhere(collectionName string, query Query) (*CommandResponse, error) {
	queryBytes, err := bson.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("bson marshal query error: %w", err)
	}

	return c.execute(cmdCollectionDeleteWhere, func(cw *connWrapper) error {
		if err := c.writeString(cw, collectionName); err != nil {
			return err
		}
		return c.writeBytes(cw, queryBytes)
	})
}
