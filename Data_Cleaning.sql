-- SQL Project - Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- 1. Melihat data awal
SELECT * FROM layoffs;

-- 2. Membuat tabel staging sebagai salinan dari tabel asli
CREATE TABLE layoffs_staging LIKE layoffs;
SELECT * FROM layoffs_staging;

-- 3. Memasukkan data dari tabel asli ke tabel staging
INSERT INTO layoffs_staging SELECT * FROM layoffs;

-- 4. Menandai duplikasi dengan ROW_NUMBER
SELECT *,
       ROW_NUMBER() OVER (PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, 
                              percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
    FROM layoffs_staging
)
SELECT * FROM duplicate_cte WHERE row_num > 1;

-- 5. Mengecek data spesifik
SELECT * FROM layoffs_staging WHERE company = 'Casper';

-- 6. Membuat tabel baru untuk pembersihan lebih lanjut
CREATE TABLE layoffs_staging2 (
    company TEXT,
    location TEXT,
    industry TEXT,
    total_laid_off INT DEFAULT NULL,
    percentage_laid_off TEXT,
    `date` TEXT,
    stage TEXT,
    country TEXT,
    funds_raised_millions INT DEFAULT NULL,
    row_num INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoffs_staging2;

-- 7. Memasukkan data dengan ROW_NUMBER
INSERT INTO layoffs_staging2 
SELECT *,
       ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, 
                          percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- 8. Menghapus duplikasi berdasarkan row_num
DELETE FROM layoffs_staging2 WHERE row_num > 1;

-- 9. Membersihkan data kolom company
UPDATE layoffs_staging2 SET company = TRIM(company);

-- 10. Normalisasi nilai industry untuk 'Crypto%'
UPDATE layoffs_staging2 SET industry = 'Crypto' WHERE industry LIKE 'Crypto%';

-- 11. Normalisasi nilai country untuk 'United States%'
UPDATE layoffs_staging2 SET country = 'United States' WHERE country LIKE 'United States%';

-- 12. Mengubah format tanggal
UPDATE layoffs_staging2 SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');
ALTER TABLE layoffs_staging2 MODIFY COLUMN `date` DATE;

-- 13. Menangani nilai NULL
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2 ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 SET industry = NULL WHERE industry = '';

-- 14. Pengecekan akhir
SELECT * FROM layoffs_staging2 WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;
SELECT * FROM layoffs_staging2 WHERE industry IS NULL OR industry = '';
SELECT * FROM layoffs_staging2 WHERE company = 'Airbnb';

-- 15. Menghapus kolom row_num karena sudah tidak diperlukan
ALTER TABLE layoffs_staging2 DROP COLUMN row_num;

-- 16. Menampilkan hasil akhir
SELECT * FROM layoffs_staging2;
