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

---
