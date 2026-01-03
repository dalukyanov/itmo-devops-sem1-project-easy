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
	"strings"
	"time"
	_ "github.com/lib/pq"
)

const (
	dbUser     = "validator"
	dbPassword = "val1dat0r"
	dbName     = "project-sem-1"
	dbHost     = "localhost"
	dbPort     = 5432
)

type StatsResponse struct {
	TotalItems      int     `json:"total_items"`
	TotalCategories int     `json:"total_categories"`
	TotalPrice      float64 `json:"total_price"`
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

	// Убедимся, что соединение закроется в случае ошибки
	defer func() {
		if err != nil && db != nil {
			_ = db.Close()
		}
	}()

	if err = db.Ping(); err != nil {
		err = fmt.Errorf("failed to connect to DB: %w", err)
		log.Fatalf("Failed to connect to DB: %v", err)
	}

	// Структура таблицы исправлена в соответствии с комментарием ревьювера
	createTable := `
		CREATE TABLE IF NOT EXISTS prices (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			category VARCHAR(255) NOT NULL,
			price DECIMAL(10,2) NOT NULL,
			create_date TIMESTAMP NOT NULL
		);
	`
	_, err = db.Exec(createTable)
	if err != nil {
		err = fmt.Errorf("failed to create table: %w", err)
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

	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		http.Error(w, "Invalid zip file", http.StatusBadRequest)
		return
	}

	// Данный фрагмент переписан, чтобы исправить косяк в автотестах, что имя файла не data.csv, а test_data.csv
	var csvFile *zip.File
	for _, file := range zipReader.File {
		if strings.HasSuffix(strings.ToLower(file.Name), ".csv") {
			csvFile = file
			break
		}
	}
	if csvFile == nil {
		http.Error(w, "No .csv file found in the archive", http.StatusBadRequest)
		return
	}

	f, err := csvFile.Open()
	if err != nil {
		http.Error(w, "Failed to open CSV file in zip", http.StatusInternalServerError)
		return
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	firstRow := true

	var records []struct {
		Name       string
		Category   string
		Price      float64
		CreateDate time.Time
	}
	categories := make(map[string]struct{})

	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			http.Error(w, "Failed to parse CSV", http.StatusBadRequest)
			return
		}

		if firstRow {
			firstRow = false
			continue // пропускаем заголовок
		}

		if len(row) != 5 {
			http.Error(w, "Invalid CSV row length (expected 5 columns)", http.StatusBadRequest)
			return
		}

		// Порядок: id, name, category, price, create_date
		name := strings.TrimSpace(row[1])
		category := strings.TrimSpace(row[2])
		priceStr := strings.TrimSpace(row[3])
		dateStr := strings.TrimSpace(row[4])

		price, err := strconv.ParseFloat(priceStr, 64)
		if err != nil {
			http.Error(w, "Invalid price format", http.StatusBadRequest)
			return
		}

		createDate, err := time.Parse("2006-01-02", dateStr)
		if err != nil {
			http.Error(w, "Invalid date format (expected YYYY-MM-DD)", http.StatusBadRequest)
			return
		}

		records = append(records, struct {
			Name       string
			Category   string
			Price      float64
			CreateDate time.Time
		}{name, category, price, createDate})

		categories[category] = struct{}{}
	}

	if len(records) == 0 {
		http.Error(w, "No data rows found in CSV", http.StatusBadRequest)
		return
	}

	tx, err := db.Begin()
	if err != nil {
		http.Error(w, "DB transaction failed", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT INTO prices(name, category, price, create_date) VALUES ($1, $2, $3, $4)`)
	if err != nil {
		http.Error(w, "Failed to prepare insert statement", http.StatusInternalServerError)
		return
	}
	defer stmt.Close()

	var totalPrice float64
	for _, rec := range records {
		_, err := stmt.Exec(rec.Name, rec.Category, rec.Price, rec.CreateDate)
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
		TotalItems:      len(records),
		TotalCategories: len(categories),
		TotalPrice:      totalPrice,
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

	rows, err := db.Query("SELECT id, name, category, price, create_date FROM prices ORDER BY id")
	if err != nil {
		http.Error(w, "Failed to query database", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var buf bytes.Buffer
	csvWriter := csv.NewWriter(&buf)

	csvWriter.Write([]string{"id", "name", "category", "price", "create_date"})

	for rows.Next() {
		var id int
		var name, category string
		var price float64
		var createDate time.Time

		if err := rows.Scan(&id, &name, &category, &price, &createDate); err != nil {
			http.Error(w, "Failed to scan row", http.StatusInternalServerError)
			return
		}

		dateStr := createDate.Format("2006-01-02")
		priceStr := fmt.Sprintf("%.2f", price)
		idStr := strconv.Itoa(id)

		if err := csvWriter.Write([]string{idStr, name, category, priceStr, dateStr}); err != nil {
			http.Error(w, "Failed to write CSV", http.StatusInternalServerError)
			return
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		http.Error(w, "CSV writer error", http.StatusInternalServerError)
		return
	}

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