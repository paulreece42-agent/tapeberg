// S3/Glacier wire protocol for Tapeberg (work in progress).
//
// Implements the subset of Amazon S3 that makes awscli work against a
// homelab tape archive:
//   ListBuckets, CreateBucket, HeadBucket, ListObjectsV2, GetBucketLocation
//   PutObject, GetObject, HeadObject, RestoreObject
// Glacier semantics: after the worker archives an object its HEAD/GET
// report storage class GLACIER; GET returns 403 InvalidObjectState until
// RestoreObject (202 Accepted) has been drained by a worker pass; the
// restored copy is pinned for the requested days (x-amz-restore header).
//
// Auth: AWS Signature Version 4 (TAPEBERG_S3_KEY / TAPEBERG_S3_SECRET).
// With no keys configured the server runs in unsigned dev mode.
// Not implemented: multipart upload, presigned URLs, versioning, ACLs,
// DeleteObject, chunked (STREAMING-*) payloads.
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

const s3XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"

// cmdS3Serve runs the S3 endpoint until interrupted.
func cmdS3Serve(cfg Config, listen string) error {
	if listen == "" {
		listen = cfg.S3Listen
	}
	if listen == "" {
		listen = ":7333"
	}
	if cfg.S3Key == "" || cfg.S3Secret == "" {
		fmt.Fprintln(os.Stderr, "s3: WARNING — no TAPEBERG_S3_KEY/SECRET set; running in UNSIGNED dev mode")
	}
	db, err := openDB(cfg.DBURL)
	if err != nil {
		return err
	}
	defer db.Close()
	s := &s3srv{cfg: cfg, db: db}
	fmt.Printf("tapeberg s3: listening on %s (path-style, bucket-in-path)\n", listen)
	return http.ListenAndServe(listen, s)
}

type s3srv struct {
	cfg Config
	db  *sql.DB
}

// ---------------- SigV4 ----------------

type s3Auth struct {
	accessKey string
	date      string
	region    string
	service   string
	signed    []string
	signature string
}

func parseSigV4(h string) (*s3Auth, bool) {
	if !strings.HasPrefix(h, "AWS4-HMAC-SHA256 ") {
		return nil, false
	}
	get := func(prefix string) string {
		for _, f := range strings.Fields(strings.TrimPrefix(h, "AWS4-HMAC-SHA256 ")) {
			if strings.HasPrefix(f, prefix) {
				return strings.TrimSuffix(strings.TrimPrefix(f, prefix), ",")
			}
		}
		return ""
	}
	cred := strings.Split(get("Credential="), "/")
	if len(cred) != 5 {
		return nil, false
	}
	return &s3Auth{
		accessKey: cred[0], date: cred[1], region: cred[2], service: cred[3],
		signed:    strings.Split(get("SignedHeaders="), ";"),
		signature: get("Signature="),
	}, true
}

func hmacSHA256(key []byte, s string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(s))
	return h.Sum(nil)
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// awsURIEncode implements the S3 flavor of RFC 3986 percent-encoding.
func awsURIEncode(s string, isPath bool) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '.', c == '_', c == '~':
			b.WriteByte(c)
		case isPath && c == '/':
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// canonicalQuery rebuilds the query string per SigV4 rules.
func canonicalQuery(raw string) string {
	if raw == "" {
		return ""
	}
	type kv struct{ k, v string }
	var pairs []kv
	for _, p := range strings.Split(raw, "&") {
		k, v, _ := strings.Cut(p, "=")
		dk, e1 := urlUnescape(k)
		dv, e2 := urlUnescape(v)
		if e1 != nil {
			dk = k
		}
		if e2 != nil {
			dv = v
		}
		pairs = append(pairs, kv{awsURIEncode(dk, false), awsURIEncode(dv, false)})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].k != pairs[j].k {
			return pairs[i].k < pairs[j].k
		}
		return pairs[i].v < pairs[j].v
	})
	parts := make([]string, len(pairs))
	for i, p := range pairs {
		parts[i] = p.k + "=" + p.v
	}
	return strings.Join(parts, "&")
}

func urlUnescape(s string) (string, error) {
	return url.QueryUnescape(s)
}

// canonicalRequest assembles the SigV4 canonical request from a live
// http.Request. Payload integrity is taken from the client-declared
// x-amz-content-sha256 (standard practice for streamed bodies).
func canonicalRequest(r *http.Request) (string, string) {
	payloadHash := r.Header.Get("x-amz-content-sha256")
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}
	headers := map[string]string{"host": r.Host}
	for name, vals := range r.Header {
		lower := strings.ToLower(name)
		// request line forms arrive as "X-Amz-Date"; map canonical names
		headers[lower] = strings.Join(vals, ",")
	}
	signed := make([]string, 0, len(headers))
	for h := range headers {
		signed = append(signed, h)
	}
	sort.Strings(signed)
	var ch strings.Builder
	for _, h := range signed {
		v := strings.TrimSpace(headers[h])
		v = strings.Join(strings.Fields(v), " ")
		ch.WriteString(h)
		ch.WriteString(":")
		ch.WriteString(v)
		ch.WriteString("\n")
	}
	signedList := strings.Join(signed, ";")
	return strings.Join([]string{
		r.Method,
		awsURIEncode(r.URL.Path, true),
		canonicalQuery(r.URL.RawQuery),
		ch.String(),
		signedList,
		payloadHash,
	}, "\n"), payloadHash
}

// verifySigV4 checks the Authorization header against TAPEBERG_S3_SECRET.
func (s *s3srv) verifySigV4(r *http.Request) error {
	if s.cfg.S3Key == "" {
		return nil // unsigned dev mode
	}
	auth, ok := parseSigV4(r.Header.Get("Authorization"))
	if !ok {
		return errS3("AccessDenied", "missing AWS SigV4 Authorization header", http.StatusForbidden)
	}
	if auth.accessKey != s.cfg.S3Key {
		return errS3("InvalidAccessKeyId", "unknown access key", http.StatusForbidden)
	}
	if auth.service != "s3" {
		return errS3("AccessDenied", "credential scope is not for s3", http.StatusForbidden)
	}
	// Rebuild the canonical request using the client's signed-headers list.
	headers := map[string]string{"host": r.Host}
	for name, vals := range r.Header {
		headers[strings.ToLower(name)] = strings.Join(vals, ",")
	}
	payloadHash := r.Header.Get("x-amz-content-sha256")
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}
	signed := append([]string(nil), auth.signed...)
	sort.Strings(signed)
	var ch strings.Builder
	for _, h := range signed {
		v := strings.TrimSpace(headers[h])
		v = strings.Join(strings.Fields(v), " ")
		ch.WriteString(h + ":" + v + "\n")
	}
	cr := strings.Join([]string{
		r.Method,
		awsURIEncode(r.URL.Path, true),
		canonicalQuery(r.URL.RawQuery),
		ch.String(),
		strings.Join(signed, ";"),
		payloadHash,
	}, "\n")
	scope := strings.Join([]string{auth.date, auth.region, auth.service, "aws4_request"}, "/")
	sts := "AWS4-HMAC-SHA256\n" + r.Header.Get("x-amz-date") + "\n" + scope + "\n" + sha256Hex([]byte(cr))
	kDate := hmacSHA256([]byte("AWS4"+s.cfg.S3Secret), auth.date)
	kRegion := hmacSHA256(kDate, auth.region)
	kService := hmacSHA256(kRegion, auth.service)
	kSigning := hmacSHA256(kService, "aws4_request")
	want := hex.EncodeToString(hmacSHA256(kSigning, sts))
	if subtle.ConstantTimeCompare([]byte(want), []byte(auth.signature)) != 1 {
		return errS3("SignatureDoesNotMatch", "signature mismatch", http.StatusForbidden)
	}
	return nil
}

// ---------------- XML shapes ----------------

type s3ErrorBody struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource"`
	RequestID string   `xml:"RequestId"`
}

type errS3T struct {
	code    string
	message string
	status  int
}

func (e *errS3T) Error() string { return e.code + ": " + e.message }

func errS3(code, msg string, status int) error {
	return &errS3T{code: code, message: msg, status: status}
}

func writeS3Error(w http.ResponseWriter, e error) {
	se, ok := e.(*errS3T)
	if !ok {
		se = &errS3T{code: "InternalError", message: e.Error(), status: http.StatusInternalServerError}
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(se.status)
	xml.NewEncoder(w).Encode(s3ErrorBody{
		Code: se.code, Message: se.message,
		Resource: "", RequestID: strconv.FormatInt(time.Now().UnixNano(), 10),
	})
}

type bucketXML struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type listBucketsXML struct {
	XMLName xml.Name    `xml:"ListAllMyBucketsResult"`
	Xmlns   string      `xml:"xmlns,attr"`
	Owner   ownerXML    `xml:"Owner"`
	Buckets []bucketXML `xml:"Buckets>Bucket"`
}

type ownerXML struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type objectXML struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type listObjectsXML struct {
	XMLName     xml.Name    `xml:"ListBucketResult"`
	Xmlns       string      `xml:"xmlns,attr"`
	Name        string      `xml:"Name"`
	Prefix      string      `xml:"Prefix"`
	KeyCount    int         `xml:"KeyCount"`
	MaxKeys     int         `xml:"MaxKeys"`
	IsTruncated bool        `xml:"IsTruncated"`
	Contents    []objectXML `xml:"Contents"`
}

type restoreRequestXML struct {
	Days int `xml:"Days"`
}

// ---------------- handlers ----------------

func (s *s3srv) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := s.verifySigV4(r); err != nil {
		writeS3Error(w, err)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/")
	bucket, key, _ := strings.Cut(path, "/")

	var err error
	switch {
	case bucket == "":
		err = s.serveRoot(w, r)
	case key == "":
		err = s.serveBucket(w, r, bucket)
	default:
		err = s.serveObject(w, r, bucket, key)
	}
	if err != nil {
		writeS3Error(w, err)
	}
}

func (s *s3srv) serveRoot(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodGet {
		return errS3("MethodNotAllowed", "operation not supported", http.StatusMethodNotAllowed)
	}
	rows, err := listBuckets(s.db)
	if err != nil {
		return err
	}
	out := listBucketsXML{Xmlns: s3XMLNS, Owner: ownerXML{ID: "tapeberg", DisplayName: "tapeberg"}}
	for _, b := range rows {
		out.Buckets = append(out.Buckets, bucketXML{Name: b.Name, CreationDate: b.Created.Format(time.RFC3339)})
	}
	w.Header().Set("Content-Type", "application/xml")
	return xml.NewEncoder(w).Encode(out)
}

func (s *s3srv) serveBucket(w http.ResponseWriter, r *http.Request, bucket string) error {
	switch r.Method {
	case http.MethodPut:
		if err := createBucketIfAbsent(s.db, bucket); err != nil {
			return err
		}
		w.Header().Set("Location", "/"+bucket)
		w.WriteHeader(http.StatusOK)
		return nil
	case http.MethodHead:
		if _, err := bucketIDByName(s.db, bucket); err != nil {
			return errS3("NoSuchBucket", "bucket does not exist", http.StatusNotFound)
		}
		w.WriteHeader(http.StatusOK)
		return nil
	case http.MethodGet:
		if r.URL.Query().Has("location") {
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprintf(w, `<LocationConstraint xmlns=%q>us-east-1</LocationConstraint>`, s3XMLNS)
			return nil
		}
		objs, err := listObjects(s.db, bucket, r.URL.Query().Get("prefix"))
		if err != nil {
			return err
		}
		out := listObjectsXML{Xmlns: s3XMLNS, Name: bucket, Prefix: r.URL.Query().Get("prefix"),
			MaxKeys: 1000, KeyCount: len(objs)}
		for _, o := range objs {
			sc := o.StorageClass
			if sc == "STANDARD" {
				sc = "" // STANDARD is the default; S3 omits it
			}
			out.Contents = append(out.Contents, objectXML{
				Key: o.Key, LastModified: o.CreatedAt.Format(time.RFC3339),
				ETag: `"` + o.SHA256 + `"`, Size: o.Size, StorageClass: sc,
			})
		}
		w.Header().Set("Content-Type", "application/xml")
		return xml.NewEncoder(w).Encode(out)
	default:
		return errS3("NotImplemented", "operation not supported (v0)", http.StatusNotImplemented)
	}
}

func (s *s3srv) serveObject(w http.ResponseWriter, r *http.Request, bucket, key string) error {
	switch {
	case r.Method == http.MethodPut && !r.URL.Query().Has("tagging"):
		return s.putObject(w, r, bucket, key)
	case r.Method == http.MethodGet || r.Method == http.MethodHead:
		return s.getObject(w, r, bucket, key)
	case r.Method == http.MethodPost && r.URL.Query().Has("restore"):
		return s.restoreObject(w, r, bucket, key)
	default:
		return errS3("NotImplemented", "operation not supported (v0)", http.StatusNotImplemented)
	}
}

func (s *s3srv) putObject(w http.ResponseWriter, r *http.Request, bucket, key string) error {
	if _, err := bucketIDByName(s.db, bucket); err != nil {
		return errS3("NoSuchBucket", "bucket does not exist", http.StatusNotFound)
	}
	obj, err := ingestStream(s.db, s.cfg, bucket, key, r.Body)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return errS3("OperationAborted", "key exists (v0 has no overwrite)", http.StatusConflict)
		}
		return err
	}
	w.Header().Set("ETag", `"`+obj.SHA256+`"`)
	w.WriteHeader(http.StatusOK)
	return nil
}

// setObjectHeaders writes the common object metadata headers.
func setObjectHeaders(w http.ResponseWriter, obj *Object, st string, exp time.Time, found bool) {
	w.Header().Set("ETag", `"`+obj.SHA256+`"`)
	w.Header().Set("Last-Modified", obj.CreatedAt.UTC().Format(http.TimeFormat))
	w.Header().Set("Accept-Ranges", "bytes")
	if obj.StorageClass == "GLACIER" || obj.StorageClass == "DEEP_ARCHIVE" {
		w.Header().Set("x-amz-storage-class", obj.StorageClass)
	}
	if found {
		switch st {
		case "restored":
			w.Header().Set("x-amz-restore",
				`ongoing-request="false", expiry-date="`+exp.UTC().Format(http.TimeFormat)+`"`)
		case "pending", "waiting_for_media", "restoring":
			w.Header().Set("x-amz-restore", `ongoing-request="true"`)
		}
	}
}

func (s *s3srv) getObject(w http.ResponseWriter, r *http.Request, bucket, key string) error {
	obj, err := getObjectIn(s.db, bucket, key)
	if err != nil {
		return errS3("NoSuchKey", "key does not exist", http.StatusNotFound)
	}
	if obj.DiskPath == "" {
		if obj.StorageClass == "GLACIER" || obj.StorageClass == "DEEP_ARCHIVE" {
			if r.Method == http.MethodHead {
				// HEAD is metadata-only and always succeeds (S3 clients
				// discover the storage class this way); GET is refused.
				st, exp, found, err := activeRetrieve(s.db, obj.ID)
				if err != nil {
					return err
				}
				setObjectHeaders(w, obj, st, exp, found)
				w.WriteHeader(http.StatusOK)
				return nil
			}
			return errS3("InvalidObjectState",
				"The operation is not valid for the object's storage class ("+obj.StorageClass+") — run restore-object first",
				http.StatusForbidden)
		}
		return errS3("InternalError", "object has no readable copy", http.StatusInternalServerError)
	}
	st, exp, found, err := activeRetrieve(s.db, obj.ID)
	if err != nil {
		return err
	}
	f, err := os.Open(obj.DiskPath)
	if err != nil {
		return err
	}
	defer f.Close()
	setObjectHeaders(w, obj, st, exp, found)
	// ServeContent handles Content-Length, ranges, and HEAD no-body.
	http.ServeContent(w, r, key, obj.CreatedAt, f)
	return nil
}

func (s *s3srv) restoreObject(w http.ResponseWriter, r *http.Request, bucket, key string) error {
	obj, err := getObjectIn(s.db, bucket, key)
	if err != nil {
		return errS3("NoSuchKey", "key does not exist", http.StatusNotFound)
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return err
	}
	var req restoreRequestXML
	if len(body) > 0 {
		if err := xml.Unmarshal(body, &req); err != nil {
			return errS3("MalformedXML", "could not parse RestoreRequest", http.StatusBadRequest)
		}
	}
	days := req.Days
	if days == 0 {
		days = 1
	}
	if days < 1 || days > 30 {
		return errS3("InvalidArgument", "Days must be 1-30", http.StatusBadRequest)
	}
	st, _, found, err := activeRetrieve(s.db, obj.ID)
	if err != nil {
		return err
	}
	if found && st == "restored" {
		w.WriteHeader(http.StatusOK) // already restored — idempotent per S3
		return nil
	}
	if found {
		return errS3("RestoreAlreadyInProgress", "object restore is already in progress", http.StatusConflict)
	}
	if err := createRetrieveRequest(s.db, obj.ID, days); err != nil {
		return err
	}
	w.WriteHeader(http.StatusAccepted)
	return nil
}

