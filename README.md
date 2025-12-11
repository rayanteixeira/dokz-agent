###  Para esse projeto utilizamos o `venv` para criar um ambiente virtual no python e ter as dependencias isoladas.

#### Primeiro passo é criar o ambiente virtual:
Em linux:
```
python3 -m venv .venv
```
Em windows:
```
python -m venv .venv
```

#### Depois basta rodar o comando abaixo para ativar o ambiente virtual:

Em linux:
```
source .venv/bin/activate
```
Em windows(PowerShell):
```
. ./.venv/Scripts/Activate.ps1
```

#### Em seguida, instale as dependencias do projeto:

```
pip install -r requirements.txt
```

### Instalar Oracle Instant Client:
Linux:
```
sudo apt-get install libaio1
```
Windows:
```
https://www.oracle.com/br/database/technologies/instant-client/winx64-64-downloads.html
```

#### Versão dinâmica do agente a cada build


Build for Windows

Powershell:
```
pyinstaller --onefile  --add-data=".env;." --add-data="scripts\query.py;script" --name "docpay_agent_v$(python -c 'import version; print(version.__version__)')" docpay_agent.py
```