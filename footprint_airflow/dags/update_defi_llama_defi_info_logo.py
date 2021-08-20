import os
import json
from config import project_config
import pandas as pd
from models import DefiProtocolInfo


def update_defi_llama_defi_info_log():
    dags_folder = project_config.dags_folder
    print(dags_folder)
    path = os.path.join(dags_folder, 'update_defi_llama_defi_info_logo.csv')
    csv_datas = pd.read_csv(path, sep=",")

    chains = csv_datas['chain'].tolist()
    slugs = csv_datas['slug'].tolist()
    logos = csv_datas['logo'].tolist()
    for index in range(0, len(slugs)):
        slug = slugs[index]
        chain = chains[index]
        logo = logos[index]
        query = {
            'slug': slug,
            'chain': chain
        }
        DefiProtocolInfo.update_one(query=query, set_dict={'logo': logo})


if __name__ == '__main__':
    update_defi_llama_defi_info_log()
