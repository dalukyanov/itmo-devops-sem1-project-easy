package main

import (
	"archive/zip"
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	_ "github.com/lib/pq"
)

const (
	dbUser     = "validator"
	dbPassword = "val1dat0r"
	dbName     = "project-sem-1"
	dbHost     = "localhost"
	dbPort     = 5432
)

type PriceRecord struct {
	ID        string
	CreatedAt string // формат: ГГГГ-ММ-ДД
	Name      string
	Category  string
	Price     int
}

type StatsResponse struct {
	TotalItems     int `json:"total_items"`
	TotalCategories int `json:"total_categories"`
	TotalPrice     int `json:"total_price"`
}

var db *sql.DB

func initDB() {
	connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%d sslmode=disable",
		dbUser, dbPassword, dbName, dbHost, dbPort)
	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}
	if err = db.Ping(); err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}

	// Создаём таблицу, если не существует
	createTable := `
		CREATE TABLE IF NOT EXISTS prices (
			id TEXT,
			created_at DATE,
			name TEXT,
			category TEXT,
			price INTEGER
		);
	`
	_, err = db.Exec(createTable)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
}

func postPricesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Читаем ZIP из памяти
	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		http.Error(w, "Invalid zip file", http.StatusBadRequest)
		return
	}

	var records []PriceRecord
	var categories = make(map[string]bool)

	for _, file := range zipReader.File {
		if file.Name != "data.csv" {
			continue
		}

		f, err := file.Open()
		if err != nil {
			http.Error(w, "Failed to open data.csv in zip", http.StatusInternalServerError)
			return
		}
		defer f.Close()

		csvReader := csv.NewReader(f)
		for {
			row, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				http.Error(w, "Failed to parse CSV", http.StatusBadRequest)
				return
			}

			if len(row) != 5 {
				http.Error(w, "Invalid CSV row length", http.StatusBadRequest)
				return
			}

			price, err := strconv.Atoi(row[4])
			if err != nil {
				http.Error(w, "Invalid price format", http.StatusBadRequest)
				return
			}

			record := PriceRecord{
				ID:        row[0],
				CreatedAt: row[1],
				Name:      row[2],
				Category:  row[3],
				Price:     price,
			}
			records = append(records, record)
			categories[record.Category] = true
		}
	}

	// Вставляем в БД
	tx, err := db.Begin()
	if err != nil {
		http.Error(w, "DB transaction failed", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO prices(id, created_at, name, category, price) VALUES ($1, $2, $3, $4, $5)`)
	if err != nil {
		http.Error(w, "Failed to prepare insert statement", http.StatusInternalServerError)
		return
	}
	defer stmt.Close()

	totalPrice := 0
	for _, rec := range records {
		_, err := stmt.Exec(rec.ID, rec.CreatedAt, rec.Name, rec.Category, rec.Price)
		if err != nil {
			http.Error(w, "Failed to insert record", http.StatusInternalServerError)
			return
		}
		totalPrice += rec.Price
	}

	if err = tx.Commit(); err != nil {
		http.Error(w, "Failed to commit transaction", http.StatusInternalServerError)
		return
	}

	resp := StatsResponse{
		TotalItems:     len(records),
		TotalCategories: len(categories),
		TotalPrice:     totalPrice,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("Failed to encode response: %v", err)
	}
}

func getPricesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	rows, err := db.Query("SELECT id, created_at, name, category, price FROM prices")
	if err != nil {
		http.Error(w, "Failed to query database", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	// Собираем CSV в памяти
	var buf bytes.Buffer
	csvWriter := csv.NewWriter(&buf)

	for rows.Next() {
		var id, createdAt, name, category string
		var price int
		if err := rows.Scan(&id, &createdAt, &name, &category, &price); err != nil {
			http.Error(w, "Failed to scan row", http.StatusInternalServerError)
			return
		}
		priceStr := strconv.Itoa(price)
		if err := csvWriter.Write([]string{id, createdAt, name, category, priceStr}); err != nil {
			http.Error(w, "Failed to write CSV", http.StatusInternalServerError)
			return
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		http.Error(w, "CSV writer error", http.StatusInternalServerError)
		return
	}

	// Создаём ZIP
	var zipBuf bytes.Buffer
	zipWriter := zip.NewWriter(&zipBuf)
	f, err := zipWriter.Create("data.csv")
	if err != nil {
		http.Error(w, "Failed to create file in zip", http.StatusInternalServerError)
		return
	}
	if _, err := f.Write(buf.Bytes()); err != nil {
		http.Error(w, "Failed to write CSV to zip", http.StatusInternalServerError)
		return
	}
	if err := zipWriter.Close(); err != nil {
		http.Error(w, "Failed to close zip", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", "attachment; filename=data.zip")
	w.Write(zipBuf.Bytes())
}

func main() {
	initDB()
	defer db.Close()

	http.HandleFunc("/api/v0/prices", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			postPricesHandler(w, r)
		case http.MethodGet:
			getPricesHandler(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("Server starting on :%s\n", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}