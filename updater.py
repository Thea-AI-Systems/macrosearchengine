import os
import json
import glob
from tools import parquet_handler, helpers

dataset_path = "datasets/"

def get_country_fullname(country_code):
    country_fullnames = {
        "IN": "India",
        "US": "United States",
        "CN": "China",
        "JP": "Japan",
        # Add more countries as needed
    }
    return country_fullnames.get(country_code)

def load_all_configs():
    configs = {}
    # Go two levels deep: datasets/*/*/config.json
    for config_path in glob.glob(os.path.join(dataset_path, '*', '*', 'config.json')):
        print(f"Reading: {config_path}")
        with open(config_path, 'r') as f:
            config = json.load(f)
            #folder name of this config file is the country
            country = os.path.basename(os.path.dirname(config_path))
            if "datasets" in config:
                datasets = config["datasets"]
                for dataset_label in datasets:
                    dataset = datasets[dataset_label]
                    dataset["local_path"] = os.path.dirname(config_path)+"/" + dataset_label+".py"
                    dataset["path"] = dataset["s3_prefix"]+"/"+country                    
                    dataset["country"] = country
                configs.update(config["datasets"])
    return configs

manually_update_datasets = [
    "IIP"
]

async def update_dataset(dataset, config, overwrite_history=False):
    file_path = config.get("local_path")
    if not file_path or not os.path.exists(file_path):        
        print(f"File path for dataset {dataset} is not valid: {file_path}")
        return
    #import the file and execute update function
    module_name = file_path.replace('/', '.').replace('.py', '')
    try:
        module = __import__(module_name, fromlist=['update'])        
        print(f"Updating dataset: {dataset}")
        await module.update(overwrite_history)
    except ImportError as e:
        print(f"Error importing module {module_name}: {e}")


async def update_datasets():
    all_configs = load_all_configs()
    tasks = []
    datasets = list(all_configs.keys())
       
    for dataset in datasets:
        config = all_configs[dataset]
        #await update_dataset(dataset, config)
        tasks.append(update_dataset(dataset, config))

    await asyncio.gather(*tasks)

async def update_datasets_manual():
    all_configs = load_all_configs()
    tasks = []
    for dataset in manually_update_datasets:            
        config = all_configs[dataset]
        #await update_dataset(dataset, config)                       
        tasks.append(update_dataset(dataset, config, True))

    await asyncio.gather(*tasks)

async def build_search_index():
    '''
        { 
            id: 1, 
            dataset: "BankCreditAndDeposits", 
            description: "Contains detailed information about overall bank credit and deposits.",
            frequency:"Weekly",
            countries:['IN'],
            country_fullnames:["India"],
            path:"Banking/BankCreditAndDeposits/IN/20250727.parquet"
        },
        { 
            id: 2, 
            dataset: "BankRatios", 
            description: "Contains detailed information about bank ratios.",
            frequency:"Weekly",
            countries:['IN'],
            country_fullnames:["India"],
            path:"Banking/BankRatios/IN/20250727.parquet"
        }
    '''
    all_parquets = await parquet_handler.get_all_parquets()
    search_index = []
    #load each config file
    all_configs = load_all_configs()
    
    for dataset, config in all_configs.items():
        if "description" not in config or "frequency" not in config:
            print(f"Skipping dataset {dataset} as it does not have required fields.")
            continue
        #search for path in parquet_handlers       
        parquet_search_path = config.get("path", "")+"/data.parquet"        
        parquet_path = next((p for p in all_parquets if p.startswith(parquet_search_path)), None)
        
        if not parquet_path:
            print(f"Skipping dataset {dataset} as no parquet file found in {parquet_search_path}")
            continue

        #check if dataset exists in search_index
        search_index.append({
            "id": len(search_index) + 1,
            "dataset": dataset,
            "description": config["description"],            
            "country": config.get("country", []),
            "country_fullname": get_country_fullname(config.get("country", [])),            
            "path": parquet_path
        })

    #save search_index to 
    search_index_path = os.path.join(dataset_path, "search_index.json")
    with open(search_index_path, "w") as f:
        json.dump(search_index, f)

if __name__ == "__main__":
    import asyncio
    asyncio.run(update_datasets_manual())
    #asyncio.run(build_search_index())
    #asyncio.run(update_datasets())
    print("All datasets updated.")
    
