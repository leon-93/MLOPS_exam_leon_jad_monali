import pandas as pd

def load_data(file_path):
    """
    Charge les données à partir d'un fichier CSV.
    """
    return pd.read_csv(file_path)

def extract_features(data):
    """
    Extrait les fonctionnalités pertinentes à partir des données.
    """
    # Exemple d'extraction de fonctionnalités
    features = data[[
        "Etiquette_DPE",
        "Etiquette_GES",
        "Type_bâtiment",
        "Surface_habitable_logement",
        "Conso_5_usages_é_finale",
        "Conso_chauffage_é_finale",
        "Conso_ECS_é_finale",
        "Emission_GES_5_usages",
        "Emission_GES_chauffage"
    ]]
    return features

def main():
    # Chargement des données
    data = load_data("./data/dpe-v2-logements-neufs.csv")

    # Extraction des fonctionnalités
    features = extract_features(data)

    # Affichage des fonctionnalités extraites
    print(features.head())
    
    # Sauvegarde des fonctionnalités extraites au format CSV
    features.to_csv("extracted_features.csv", index=False)

if __name__ == "__main__":
    main()
