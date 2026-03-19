import requests
import json

def scrap(name: str, url: str, output_file_type: str = 'html', return_data: bool = False):
    try:
        headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}    
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        data = response.text

        if response.status_code == 200:            

            if output_file_type == 'html':
                with open(f'{name}.html', 'w', encoding='utf-8') as file:
                    file.write(data)
                
            elif output_file_type == 'json':
                with open(f'{name}.json', 'w', encoding='utf-8') as file:                 
                    data = json.loads(data)
                    json.dump(data, file, indent=2)                                
            else:
                print("Invalid output file type")            

            if return_data:
                return data
            
        else:
            print(f"Fail status codes: {response.status_code}")
        
    except Exception as e:
        print(e)