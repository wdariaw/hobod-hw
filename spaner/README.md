Порядок запуска:

0. Поднятый локально Spanner (https://cloud.google.com/spanner/docs/emulator), т.к. карта не принимается и trial не смогла получить
1. python3 create_db.py
2. python3 insert_data.py (для локального применения запускала на части данных, иначе таймауты и слишком долго)
3. python3 select_data.py + через несколько секунд python3 update_data.py (выход -- before.txt)
4. python3 select_data.py (выход -- after.txt)
