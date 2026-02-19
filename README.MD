# 📚 KUP – Eksport i archiwizacja rekordów PBL

Skrypt w Pythonie do pobierania rekordów z bazy Oracle, filtrowania ich na podstawie użytkownika i dat, dzielenia na mniejsze paczki oraz automatycznego zapisywania w plikach `.ods` i przesyłania na **Google Drive**.  
Dodatkowo skrypt aktualizuje arkusz Google Sheets, aby zachować historię wszystkich już przetworzonych rekordów.  
Na koniec wygenerowane pliki należy przesłać mailowo do odpowiedniego pracownika zgodnie z ustalonymi zasadami.

---

## ⚙️ Funkcjonalności

- Pobiera dane z bazy Oracle (połączenie przez `oracledb`; dawniej przez `cx_Oracle`).
- Inicjalizuje Oracle Client w trybie thick, jeśli jest potrzebny do połączenia ze starszymi wersjami serwera.
- Filtruje rekordy według użytkownika i daty.
- Sprawdza, które rekordy zostały już wcześniej zapisane (na podstawie Google Sheets).
- Dzieli dane na mniejsze pliki `.ods` (200–220 wierszy każdy).
- Zapisuje pliki lokalnie w folderze `data/`.
- Wysyła pliki do wybranego folderu na Google Drive.
- Aktualizuje arkusz Google Sheets z listą przetworzonych ID.


## 🛠️ Wymagania

- 🐍 Python 3.12+
- 📦 Zainstalowane paczki: oracledb, pandas, PyDrive, gspread, gspread_dataframe
- 🔑 Konto Google z dostępem do Google Drive, w którym będą zapisywane pliki
- 🔑 Zmienne środowiskowe odpowiadające danym dostępowym do bazy danych PBL ('PBL_ORACLE_HOST', 'PBL_ORACLE_PORT', 'PBL_ORACLE_SERVICE', 'PBL_ORACLE_USER', 'PBL_ORACLE_PASSWORD'); hasła znajdują się tutaj: 
- 📄 Plik `client_secrets.json` w katalogu z kodem (plik z danymi uwierzytelniającymi OAuth 2.0 dla Google Drive).
  - Można go pobrać z [Google Cloud Console](https://console.cloud.google.com/apis/credentials)
  - Musi być poprawnie skonfigurowany, aby skrypt mógł przesyłać pliki na Google Drive
- 💾 Oracle Instant Client i zmienna środowiskowa **PBL_ORACLE_LIB_DIR** wskazująca folder z Instant Client
  - Jest potrzebny, jeśli serwer Oracle jest starszy i wymaga połączenia w trybie thick
  - Skrypt automatycznie wywołuje oracledb.init_oracle_client(lib_dir=…) przy thick mode
  - Pobranie Instant Client: [Oracle Instant Client Downloads](https://www.oracle.com/database/technologies/instant-client.html)
  - Wybierz wersję zgodną z systemem operacyjnym (np. Windows 64-bit)
  - Rozpakuj do wybranego folderu i ustaw zmienną środowiskową **PBL_ORACLE_LIB_DIR** na jego ścieżkę
- Należy stworzyć folder `data/` tuż po pobraniu tego repozytorium.

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
