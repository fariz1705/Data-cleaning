-- Sumber data: https://www.kaggle.com/datasets/ulrikeherold/tech-layoffs-2020-2024

USE layoffs_tech;

-- Mengecek isi tabel awal
SELECT * FROM tech_layoffs;

-- Membuat tabel staging dengan struktur yang sama
CREATE TABLE tech_layoffs_staging LIKE tech_layoffs;

-- Mengecek isi tabel staging
SELECT * FROM tech_layoffs_staging ORDER BY Company ASC;

-- Mengubah nama kolom yang salah encoding
ALTER TABLE tech_layoffs_staging RENAME COLUMN ï»¿Nr TO Id;

-- Mengecek hasil perubahan nama kolom
SELECT * FROM tech_layoffs_staging;

-- Memindahkan data dari tech_layoffs ke tech_layoffs_staging
INSERT INTO tech_layoffs_staging SELECT * FROM tech_layoffs;

-- Mendeteksi duplikasi berdasarkan semua kolom utama
SELECT company, location_hq, region, usstate, country, continent, laid_off, date_layoffs, 
       company_size_before_layoffs, company_size_after_layoffs, industry, stage, 
       money_raised_in__mil, `year`, latitude, longitude, COUNT(*) 
FROM tech_layoffs_staging
GROUP BY company, location_hq, region, usstate, country, continent, laid_off, date_layoffs, 
         company_size_before_layoffs, company_size_after_layoffs, industry, stage, 
         money_raised_in__mil, `year`, latitude, longitude 
HAVING COUNT(*) > 1;

-- Menghapus duplikasi dengan Common Table Expression (CTE)
WITH CTE AS (
    SELECT id, 
           ROW_NUMBER() OVER (
               PARTITION BY company, location_hq, region, usstate, country, continent, laid_off, 
                            date_layoffs, company_size_before_layoffs, company_size_after_layoffs, 
                            industry, stage, money_raised_in__mil, `year`, latitude, longitude 
               ORDER BY id
           ) AS row_num
    FROM tech_layoffs_staging
)
DELETE FROM tech_layoffs_staging
WHERE id IN (SELECT id FROM CTE WHERE row_num > 1);

-- Mengecek perusahaan tertentu yang mungkin memiliki data duplikat
SELECT * FROM tech_layoffs_staging WHERE company = 'Planet';
SELECT * FROM tech_layoffs_staging WHERE company = 'Wex';

-- Mengecek nilai kosong pada kolom Company_Size_after_layoffs
SELECT * FROM tech_layoffs_staging 
WHERE Company_Size_after_layoffs = '';

-- Mengubah nilai kosong menjadi 0 jika semua karyawan di-PHK
UPDATE tech_layoffs_staging
SET Company_Size_after_Layoffs = 0
WHERE Laid_Off = Company_Size_before_Layoffs 
AND Company_Size_after_Layoffs = '';

-- Mengonversi kolom date_layoffs dari teks ke tipe DATE
UPDATE tech_layoffs_staging
SET date_layoffs = STR_TO_DATE(date_layoffs, '%d.%m.%y');

-- Mengubah tipe data agar lebih sesuai
ALTER TABLE tech_layoffs_staging MODIFY date_layoffs DATE;
ALTER TABLE tech_layoffs_staging MODIFY year YEAR;
ALTER TABLE tech_layoffs_staging MODIFY Company_Size_after_layoffs INT;

-- Mengecek hasil akhir dari staging
SELECT * FROM tech_layoffs_staging;

-- Membuat tabel cleaned tanpa duplikasi
CREATE TABLE tech_layoffs_cleaned AS 
SELECT DISTINCT * FROM tech_layoffs_staging;

-- Mengecek tabel hasil cleaning
SELECT * FROM tech_layoffs_cleaned;
