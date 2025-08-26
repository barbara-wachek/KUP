# 📚 KUP – Eksport i archiwizacja rekordów PBL

Skrypt w Pythonie do pobierania rekordów z bazy Oracle, filtrowania ich na podstawie użytkownika i dat, dzielenia na mniejsze paczki oraz automatycznego zapisywania w plikach `.ods` i przesyłania na **Google Drive**.  
Dodatkowo skrypt aktualizuje arkusz Google Sheets, aby zachować historię wszystkich już przetworzonych rekordów.  
Na koniec wygenerowane pliki należy przesłać mailowo do odpowiedniego pracownika zgodnie z ustalonymi zasadami.

---

## ⚙️ Funkcjonalności

- Pobiera dane z bazy Oracle (połączenie przez `cx_Oracle`).
- Filtruje rekordy według użytkownika i daty.
- Sprawdza, które rekordy zostały już wcześniej zapisane (na podstawie Google Sheets).
- Dzieli dane na mniejsze pliki `.ods` (200–220 wierszy każdy).
- Zapisuje pliki lokalnie w folderze `data/`.
- Wysyła pliki do wybranego folderu na Google Drive.
- Aktualizuje arkusz Google Sheets z listą przetworzonych ID.


## 🛠️ Wymagania

- 🐍 Python 3.10+
- 📦 Zainstalowane paczki: cx_Oracle, pandas, PyDrive, gspread, gspread_dataframe

## 🔄 Workflow (działania użytkownika)

1. Otwórz terminal (cmd) w folderze z kodem.  
2. Uruchom skrypt poleceniem:
   ``` bash
   python KUP.py
   ```

3. Wpisz nazwę użytkownika, gdy pojawi się komunikat.
4. Skrypt automatycznie pobierze dane, podzieli je na pliki `.ods` i wyśle je na Google Drive i lokalnego folderu `data/`.
5. Wygenerowane pliki należy następnie wysłać mailowo do odpowiedniego pracownika.

---
