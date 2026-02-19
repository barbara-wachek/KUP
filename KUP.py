#%% Import
import oracledb
import pandas as pd
import os
import datetime
import random

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from gspread_dataframe import set_with_dataframe, get_as_dataframe
import gspread as gs



#%% Connection to DB

# Inicjalizacja thick mode
oracledb.init_oracle_client(
    lib_dir=r"C:\Oracle\instantclient_19_29"  # ścieżka do Oracle Instant Client
)

def connect_to_database():
    host = os.environ.get('PBL_ORACLE_HOST')
    port = os.environ.get('PBL_ORACLE_PORT')
    service = os.environ.get('PBL_ORACLE_SERVICE')
    user = os.environ.get('PBL_ORACLE_USER')
    password = os.environ.get('PBL_ORACLE_PASSWORD')

    dsn = f"{host}:{port}/{service}"

    try:
        connection = oracledb.connect(
            user=user,
            password=password,
            dsn=dsn
        )
        print("Połączono z bazą w trybie thick!")
        return connection
    except oracledb.Error as error:
        print(f"Error connecting to database: {error}")
        return None

#%% Functions

def authorize_gdrive():
    #autoryzacja do penetrowania dysku
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()
    drive = GoogleDrive(gauth)
    return drive


def gsheet_to_df(gsheetId, worksheet):
    gc = gs.oauth()
    sheet = gc.open_by_key(gsheetId)
    df = get_as_dataframe(sheet.worksheet(worksheet), evaluate_formulas=True, dtype=str).dropna(how='all').dropna(how='all', axis=1)
    return df

#Wybór nazwy użytkownika z predefiniowanej listy
def get_user_pbl():
    proper_users = ['KAROLINA', 'IZA', 'OLA', 'GOSIA', 'BEATAD', 'BEATAS', 'TOMASZ', 'EWA', 'BARBARAW', 'BEATAK', 'NIKODEM', 'CEZARY', 'TOMASZU', 'PAULINA']

    while True:
        answer = input("Wpisz nazwę użytkownika: ")
        if answer in proper_users:
            user_pbl = answer
            print(f"Wybrałeś: {answer}")
            return user_pbl  # wychodzimy z pętli, bo wpisano poprawną wartość
        else:
            print("Błąd: wpisano niedozwoloną wartość. Spróbuj ponownie.")
        
        

def execute_query(conn, query, params):
    cursor = conn.cursor()
    cursor.execute(query, params)
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(cursor.fetchall(), columns=columns)
    cursor.close()
    return df

def split_dataframe(df, min_rows=200, max_rows=220):
    """Dzieli DataFrame na mniejsze DataFrame'y o liczbie wierszy z przedziału [min_rows, max_rows]."""

    result = []
    start_index = 0
    n = len(df)
    
    if n <= min_rows:
        print("Ostrzeżenie: DataFrame jest za mały, aby go podzielić na kilka plików.")
        return [df]
    
    while start_index < n:
        # losowa liczba wierszy w przedziale min_rows–max_rows, ale nie więcej niż pozostało
        chunk_size = min(random.randint(min_rows, max_rows), n - start_index)
        chunk = df.iloc[start_index:start_index + chunk_size]
        result.append(chunk)
        start_index += chunk_size
    
    return result


def upload_to_drive(drive, local_file_path, folder_id):
    filename = os.path.basename(local_file_path)
    gfile = drive.CreateFile({'parents': [{'id': folder_id}], 'title': filename})
    gfile.SetContentFile(local_file_path)  # tutaj pełna ścieżka lokalna
    gfile.Upload()
    print(f"Przesłano plik na Google Drive: {filename}")


#%% Main

if __name__ == "__main__":
    
    user_pbl = get_user_pbl()
    file_with_user_records = '1DpHG81z-HCu1JT4mG2whMxWZ83Zas9j6CerZ36HsGFY'  #ten dokument należy wypełnić ID - NW - JSON
    date_start = '2024/01/01'
    date_end = datetime.date.today().strftime('%Y/%m/%d')

    query = '''
    select distinct z.za_zapis_id as ID, 
           z.za_type as TYP,
           r.rz_nazwa as RODZAJ, 
           d.dz_nazwa as DZIAŁ,
           t.am_nazwisko as AUTOR_NAZWISKO, 
           t.am_imie as AUTOR_IMIE, 
           z.za_tytul as TYTUL, 
           z.za_adnotacje AS ADNOTACJA, 
           zr.zr_tytul as CZASOPISMO, 
           z.za_zrodlo_rok AS CZASOPISMO_ROK,
           z.za_zrodlo_nr AS CZASOPISMO_NUMER, 
           z.za_zrodlo_str AS CZASOPISMO_STRONY, 
           z.za_rok_wydania as KSIAZKI_ROK_WYDANIA, 
           z.za_wydanie as KSIAZKI_WYDANIE, 
           z.za_opis_fizyczny_ksiazki as KSIAZKI_OPIS_FIZYCZNY,
           z.za_seria_wydawnicza as KSIAZKI_SERIA_WYDAWNICZA,
           z.za_opis_wspoltworcow as KSIAZKI_OPIS_WSPOLTWORCOW,
           z.za_wydawnictwa as KSIAZKI_WYDAWNICTWA   
    from IBL_OWNER.pbl_zapisy z
    left join IBL_OWNER.pbl_zrodla zr on z.za_zr_zrodlo_id = zr.zr_zrodlo_id
    left join IBL_OWNER.pbl_zapisy_autorzy a on z.za_zapis_id = a.zaam_za_zapis_id
    left join IBL_OWNER.pbl_dzialy d on z.za_dz_dzial1_id = d.dz_dzial_id
    left join IBL_OWNER.pbl_autorzy t on a.zaam_am_autor_id = t.am_autor_id
    left join IBL_OWNER.pbl_rodzaje_zapisow r on z.za_rz_rodzaj1_id = r.rz_rodzaj_id
    where upper(z.za_uzytk_wpisal) like upper(:user_pbl)
      and z.za_uzytk_wpis_data between TO_DATE(:date_start, 'YYYY/MM/DD') 
                                  and TO_DATE(:date_end, 'YYYY/MM/DD')
    order by z.za_zapis_id
    '''
    
    
    
    with connect_to_database() as conn:
        if conn:
            
            params = {
            "user_pbl": user_pbl,
            "date_start": date_start,
            "date_end": date_end
            }
            
            df = execute_query(conn, query, params)
                      
            existing_df = gsheet_to_df(file_with_user_records, 'Arkusz1')
            used_id_list = existing_df['ID'].dropna().astype(str).tolist()
            final_df = df[~df['ID'].astype(str).isin(used_id_list)].copy()
             
            # Zapisz do pliku dataframe ze wszystkimi niewykorzystanymi dotychczas rekordami (zamień potem na format ods)
            
            
            if not final_df.empty:
                
                # autoryzacja Google Drive
                drive = authorize_gdrive()
                drive_folder_id = "1nsAOW27M_3Q3Izg3fWhoo8rT8n24kQLr"  
                
                # Podział final_df na mniejsze pliki
                chunks = split_dataframe(final_df, min_rows=200, max_rows=220)

                
                for i, chunk in enumerate(chunks, start=1):
                    # nazwa pliku (tylko nazwa, bez ścieżki)
                    local_filename = f"{user_pbl}_{datetime.date.today().strftime('%Y-%m-%d')}_part{i}.ods"
                    
                    # pełna ścieżka do zapisu w folderze data
                    local_file_path = os.path.join("data", local_filename)
                    os.makedirs("data", exist_ok=True)  # upewnij się, że folder istnieje
                    
                    # zapis pliku
                    chunk.to_excel(local_file_path, index=False, engine='odf')
                    print(f"Zapisano plik: {local_file_path}")
                                
                    # upload na Google Drive (z użyciem pełnej ścieżki)
                    upload_to_drive(drive, local_file_path, drive_folder_id)     
                                    
                # Aktualizacja pliku ze wszystkimi ID
                final_df_only_ID = pd.DataFrame({'ID': final_df['ID'].astype(str).tolist()})
                combined_df = pd.concat([existing_df, final_df_only_ID], ignore_index=True)
                
                # Nadpisanie zawartości w arkuszu Google Sheets
                gc = gs.oauth()
                sheet = gc.open_by_key(file_with_user_records).worksheet('Arkusz1')
                sheet.clear()  # usuń stare dane
                set_with_dataframe(sheet, combined_df)  # zapisz nowe dane
                
                print(f"Zaktualizowano plik z wszystkimi rekordami: {file_with_user_records}")
            else:
                print("Brak nowych rekordów do zapisania.")
        else:
            print("Nie można połączyć się z bazą.")














