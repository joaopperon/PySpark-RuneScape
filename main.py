from Core.constants import *
from Core.pyspark import PySparkRuneScape
from Core.utils import erase_input_files
from Core.ingestion import get_runescape_data

if __name__ == "__main__":

    # Pega dados fonte
    get_runescape_data(10)

    # Instancia classe de pyspark
    rs = PySparkRuneScape()

    # Filtra monstros para "membros"
    members_monsters = rs.filter_data(rs.source_data, "members", "true")
    members_monsters.show()

    # Ordena monstros para "membros" por released_date
    members_monsters_ordered = rs.order_by_release(members_monsters)
    members_monsters_ordered.show()

    # Conta quantos monstros são para "membros" e quantos não
    members_monsters_count = rs.group_by_count(rs.source_data, "members")
    members_monsters_count.show()

    # Converte dados em JSON para Parquet
    rs.convert_to_parquet(rs.source_data)

    # Apaga arquivos fonte
    erase_input_files()