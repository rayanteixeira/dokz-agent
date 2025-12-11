import logging
import boto3
import cx_Oracle as oracledb
import os
import pandas as pd
from scripts import query
from dotenv import load_dotenv, find_dotenv
from datetime import datetime, timedelta
import zipfile
import sys
import requests
import json

pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 1000)

pd.set_option('future.no_silent_downcasting', True)

def create_directory():

    output = os.getenv("DIR_FILES")
    logs = 'logs'
    try:
        if os.path.exists(output) and os.path.isdir(output):
            print(f"{output} already exists")
        else:
            os.makedirs(logs, exist_ok=True)
            os.makedirs(output, exist_ok=True)
            print(f"Directories {output} and {logs} created successfully.")
        return output, logs
    except Exception as e:
        print(f"❌ Error creating directories: {e}")
        sys.exit(1)

def configure_pool():
   
    pool = oracledb.SessionPool(user=db_user,
                                password=db_pass,
                                dsn=db_dsn,
                                min=2,
                                max=5,
                                increment=1,
                                encoding="UTF-16")
    
    return pool

def read_file_load(file_load):
    with open(file_load, "r") as arquivo:
        return arquivo.read()

def write_file_load(file_load, dt_execucao_carga_str):
    with open(file_load, "w") as arquivo:
        arquivo.write(dt_execucao_carga_str)
    log_message(f"\nArquivo {file_load} criado com sucesso!")

def file_exists(file_load):
    return os.path.exists(file_load)

def save_data(data, filename, ambiente):
    
    output_filename = f'{output}/{filename}'
    
    data['FILENAME'] = filename
    data['ID_INSTITUICAO_SAUDE'] = v_instituicao_saude
    data['DT_INI_CARGA'] = dt_execucao_carga_str
    data['DT_INGESTAO'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
     
    if ambiente == "test":
        save_to_csv(data, output_filename)
        #save_to_json(data, output_filename, orient='index', lines=False)
        #save_to_parquet(data, output_filename)
    elif ambiente in ["dev", "prod"]:
        save_to_csv(data, output_filename)
        #save_to_parquet(data, output_filename)
        #save_to_json(data, output_filename)

def save_to_csv(data, filename):
    data.to_csv(f'{filename}.csv', index=False, sep='|', encoding='UTF-16')
    log_message(f"Arquivo CSV criado com sucesso: {filename}")

def save_to_json(data, filename, orient='records', lines=True):
    data.to_json(f'{filename}.json', orient=orient, lines=lines, force_ascii=False, date_format='iso', indent=4)#,  compression='gzip')
    log_message(f"Arquivo JSON criado com sucesso: {filename}")

def save_to_parquet(data, filename):
    data.to_parquet(f'{filename}.parquet', compression='snappy', engine='pyarrow')
    log_message(f"Arquivo Parquet criado com sucesso: {filename}")

def upload_to_s3(output_filename, bucket, s3_path):
    s3 = boto3.client('s3')
    s3.upload_file(output_filename, bucket, s3_path)

def ifnull(var, val):
    if var is None:
        return val
    return var

def anonymize(nome):
    partes = nome.split()
    palavras_ignoradas = ['da', 'de', 'do', 'dos']
    partes_filtradas = [p for p in partes if p not in palavras_ignoradas]
    if len(partes_filtradas) < 3:
        return ' '.join([p[0] + '.' for p in partes_filtradas])
    else:
        return f"{partes_filtradas[0]} {partes_filtradas[1][0]}. {partes_filtradas[2][0]}."

def excluir_arquivos_em_diretorio(diretorio, pasta=True):
    if pasta:
        for arquivo in os.listdir(diretorio):
            caminho_arquivo = os.path.join(diretorio, arquivo)
            try:
                if os.path.isfile(caminho_arquivo):
                    os.unlink(caminho_arquivo)
                    log_message(f"Arquivo excluído: {caminho_arquivo}")
                elif os.path.isdir(caminho_arquivo):
                    # Caso você também queira remover diretórios vazios
                    os.rmdir(caminho_arquivo)
                    log_message(f"Diretório vazio excluído: {caminho_arquivo}")
            except Exception as e:
                log_message(f"Erro ao excluir {caminho_arquivo}: {e}")
    else:
        caminho_arquivo = diretorio
        try:
            if os.path.isfile(caminho_arquivo):
                os.unlink(caminho_arquivo)
                log_message(f"Arquivo excluído: {caminho_arquivo}")
            elif os.path.isdir(caminho_arquivo):
                # Caso você também queira remover diretórios vazios
                os.rmdir(caminho_arquivo)
                log_message(f"Arquivo excluído: {caminho_arquivo}")
        except Exception as e:
            log_message(f"Erro ao excluir {caminho_arquivo}: {e}")
    
def execute_script_prod_medica(connection, list_cd_pessoa_fisica_medicos):
    cursor = connection.cursor()

    list_prod_medica = []
    if (tipo_carga == 'full') :
        print("Iniciando carga full de produções médicas...")
        for result in cursor.execute(query.getAllProdMedica(data_inicio_carga, data_fim_carga, list_cd_pessoa_fisica_medicos)):
            list_prod_medica.append(result)
    else :
        print("Iniciando carga incremental de produções médicas...")
        for result in cursor.execute(query.getAllProdMedica_incr(data_inicio_carga, data_fim_carga,list_cd_pessoa_fisica_medicos)):
            list_prod_medica.append(result)
    
    cursor.close()
    return list_prod_medica

def execute_script_previsao_regra_repasse(df_repasse_s_regra, connection):


    int_cols = ['CD_CONVENIO_PARAMETRO', 'CD_EDICAO_AMB', 'CD_ESTABELECIMENTO',
                'CD_MEDICO_RESP', 'IE_TIPO_ATENDIMENTO', 'NR_SEQ_ETAPA_CHECKUP',
                'NR_SEQ_PROCED']
    
    for col in int_cols:
        df_repasse_s_regra[col] = pd.to_numeric(df_repasse_s_regra[col], errors='coerce').astype('Int64')

    if df_repasse_s_regra.empty:
        log_message("DataFrame de repasse sem regra está vazio. Retornando.")
        return df_repasse_s_regra

    list_previsao = []
    
    cursor = connection.cursor()

    OUTPUT_CD_REGRA_P = cursor.var(int)
    OUTPUT_NR_SEQ_CRITERIO_P = cursor.var(int)

    loop_proc = 0
    
    for index, row in df_repasse_s_regra.iterrows():
        try:
            CD_CONVENIO_PARAMETRO = row.get('CD_CONVENIO_PARAMETRO')
            CD_EDICAO_AMB = row.get('CD_EDICAO_AMB')
            CD_ESTABELECIMENTO = row.get('CD_ESTABELECIMENTO')
            CD_MEDICO_RESP =row.get('CD_MEDICO_RESP')
            IE_TIPO_ATENDIMENTO = row.get('IE_TIPO_ATENDIMENTO')
            NR_SEQ_ETAPA_CHECKUP = row.get('NR_SEQ_ETAPA_CHECKUP')
            NR_SEQ_PROCED = row.get('NR_SEQ_PROCED')

            CD_MEDICO_EXEC_P = row.get('NVL_CD_MEDICO_EXEC_REPASSE')
            CD_CGC_PRESTADOR = row.get('CD_CGC_PRESTADOR')
            IE_FUNCAO_MEDICO = row.get('IE_FUNCAO_MEDICO')
            IE_PARTICIPOU_SUS = row.get('IE_PARTICIPOU_SUS')
            IE_RESPONSAVEL_CREDITO = row.get('IE_RESPONSAVEL_CREDITO')
            IE_TIPO_ATO_SUS_P = row.get('IE_TIPO_ATO_SUS')
            IE_TIPO_SERVICO_SUS_P = row.get('IE_TIPO_SERVICO_SUS')
            NM_USUARIO_ORIGINAL = row.get('NM_USUARIO_ORIGINAL')
            NR_SEQ_PARTIC = row.get('NR_SEQ_PARTIC')
            
            cols_input_regra = (
                CD_CONVENIO_PARAMETRO, CD_EDICAO_AMB, CD_ESTABELECIMENTO, CD_MEDICO_RESP,
                CD_MEDICO_EXEC_P, CD_CGC_PRESTADOR, IE_FUNCAO_MEDICO, IE_PARTICIPOU_SUS,
                IE_RESPONSAVEL_CREDITO, IE_TIPO_ATENDIMENTO, IE_TIPO_ATO_SUS_P,
                IE_TIPO_SERVICO_SUS_P, NM_USUARIO_ORIGINAL, NR_SEQ_ETAPA_CHECKUP,
                NR_SEQ_PARTIC, NR_SEQ_PROCED,
                OUTPUT_CD_REGRA_P, OUTPUT_NR_SEQ_CRITERIO_P
            )
            
            cursor.callproc('tasy.obter_regra_proc_repasse', cols_input_regra)
            
            list_result_func = [
                NR_SEQ_PROCED,
                NR_SEQ_PARTIC,
                OUTPUT_CD_REGRA_P.getvalue(),
                OUTPUT_NR_SEQ_CRITERIO_P.getvalue()
            ]
            list_previsao.append(list_result_func)

            loop_proc += 1
            if loop_proc % 1000 == 0:
                log_message(f"Processados {loop_proc} procedimentos para previsão de regra de repasse.")

        except oracledb.Error as e:
            log_message(f"❌ Erro do Oracle ao processar NR_SEQ_PROCED {NR_SEQ_PROCED}: {e}", 'error')
            # Continua para o próximo item do loop após o erro

        except Exception as e:
            log_message(f"❌ Erro geral ao processar NR_SEQ_PROCED {NR_SEQ_PROCED}: {e}", 'error')
            # Continua para o próximo item do loop após o erro

    cursor.close()
    
    header_sregra = ['NR_SEQ_PROCED', 'NR_SEQ_PARTIC', 'CD_REGRA_PREVISTO', 'NR_SEQ_CRITERIO_PREVISTO']
    df_previsao = pd.DataFrame(list_previsao, columns = header_sregra)

    # O merge deve ser feito usando a coluna NR_SEQ_PARTIC como string ou número (garantir consistência de tipo)
    df_previsao_regra = df_repasse_s_regra.merge(df_previsao, on=['NR_SEQ_PROCED', 'NR_SEQ_PARTIC'], how='left')

    return df_previsao_regra
   
def verifica_regra(row):

    val_proc_pacote = 0 if pd.isnull(row.get('NR_SEQ_PROC_PACOTE')) else int(row.get('NR_SEQ_PROC_PACOTE'))
    val_proced = 0 if pd.isnull(row['NR_SEQ_PROCED']) else int(row['NR_SEQ_PROCED'])
    val_regra_1 = 0 if pd.isnull(row.get('REGRA_PACOTE_1')) else int(row.get('REGRA_PACOTE_1'))
    val_regra_2 = 0 if pd.isnull(row.get('REGRA_PACOTE_2')) else int(row.get('REGRA_PACOTE_2'))
    
    if val_proc_pacote == 0:
        return 'N'
    elif val_proc_pacote != val_proced and val_regra_1 == 1:
        return 'N'
    elif val_proc_pacote == val_proced and val_regra_2 == 1:
        return 'N'
    else:
        return 'S'

def script0_obter_cd_medico(connection, list_cpf_medicos):

    cpf = [f"'{cpf}'" for cpf in list_cpf_medicos]
    cpf_formated = ', '.join(cpf)
    list_cpf = f'({cpf_formated})'

    cursor = connection.cursor()

    list_pessoa = []
    for result in cursor.execute(query.getCDMedico(list_cpf)):
        list_pessoa.append(result)

    header_pessoa = ['CD_PESSOA_FISICA', 'NM_MEDICO', 'NR_CPF']
    tbl_dados_medico = pd.DataFrame(list_pessoa, columns=header_pessoa)
    
    cursor.close()

    return tbl_dados_medico

def script1_obter_prod_medica(connection, df_cd_pessoa_fisica):

    list_values = df_cd_pessoa_fisica['CD_PESSOA_FISICA'].astype(str).tolist()
    ids = [f"'{id}'" for id in list_values]
    ids_formated = ', '.join(ids)
    list_cd_pessoa_fisica_medicos = f'({ids_formated})'

    list_prod_medica = execute_script_prod_medica(connection, list_cd_pessoa_fisica_medicos)

    if not list_prod_medica:
        log_message("Sem produção médica para o período e médicos informado.")
        return pd.DataFrame()
    else:
        header_prod_med = ['NR_SEQ_PROCED', 'NR_SEQ_PROCED_REPASSE', 'NR_SEQ_PARTIC', 'DT_ATUALIZACAO_PP', 'DT_ATUALIZACAO_CP',
                            'DT_ATUALIZACAO_PR', 'DT_ATUALIZACAO_RNF', 'DT_ATUALIZACAO_PPART', 'DT_ATUALIZACAO_RT', 'CD_TAXA', 'DS_TAXA',
                            'CD_SETOR_ATENDIMENTO', 'DS_SETOR_ATENDIMENTO', 'CD_PROCEDIMENTO', 'IE_ORIGEM_PROCED', 'IE_CLASSIFICACAO', 'NR_PRESCRICAO',
                            'NR_CIRURGIA', 'NR_SEQ_PROC_PACOTE', 'NR_ATENDIMENTO', 'CD_MEDICO_RESP', 'DT_CONTA', 'DT_PROCEDIMENTO',
                            'CD_MEDICO_LAUDO', 'NR_INTERNO_CONTA', 'NR_SEQ_PROC_INTERNO', 'DT_ALTA', 'DT_MESANO_REFERENCIA',
                            'DT_ENTRADA', 'CD_CONVENIO_PARAMETRO', 'DS_CONVENIO', 'CD_EDICAO_AMB', 'CD_ESTABELECIMENTO', 'DS_ESTABELECIMENTO', 'CD_MEDICO_EXECUTOR',
                            'CD_MEDICO_REPASSE', 'CD_CGC_PRESTADOR', 'IE_FUNCAO_MEDICO', 'IE_PARTICIPOU_SUS',
                            'IE_RESPONSAVEL_CREDITO', 'IE_TIPO_ATENDIMENTO', 'IE_TIPO_ATO_SUS', 'IE_TIPO_SERVICO_SUS',
                            'NM_USUARIO_ORIGINAL', 'NR_SEQ_ETAPA_CHECKUP', 'VL_LIBERADO', 'QT_PROCEDIMENTO', 'VL_PROCEDIMENTO',
                            'VL_PARTICIPANTE', 'VL_MEDICO', 'VL_ANESTESISTA', 'VL_MATERIAIS', 'VL_AUXILIARES', 'VL_CUSTO_OPERACIONAL',
                            'DS_PROC_INTERNO', 'NM_MEDICO_EXEC', 'DS_FUNCAO_MEDICO', 'CD_ESPECIALIDADE', 'DS_ESPECIALIDADE', 'NR_PROTOCOLO',
                            'NR_SEQ_PROTOCOLO', 'IE_STATUS_PROTOCOLO', 'DT_DEFINITIVO','IE_STATUS_ACERTO', 'NM_PACIENTE', 'CD_REGRA_REPASSE',
                            'NR_SEQ_CRITERIO_REPASSE', 'NR_REPASSE_TERCEIRO', 'NR_NOTA_FISCAL', 'NR_SEQ_NOTA_FISCAL',
                            'DT_APROVACAO_TERCEIRO', 'VL_CONTA', 'VL_REPASSE', 'ITEM_AUDIT', 'CD_MOTIVO_EXC_CONTA',
                            'CD_TIPO_PROCEDIMENTO', 'DS_TIPO_PROCEDIMENTO', 'DS_PROCEDIMENTO', 'CD_GRUPO_PROC', 'CD_TIPO_ACOMODACAO',
                            'IE_ATENDIMENTO_RETORNO', 'DS_MOTIVO_EXC_CONTA', 'DT_PERIODO_INICIAL', 'DT_PERIODO_FINAL',
                            'NR_SEQ_ORIGEM', 'IE_STATUS', 'DT_ULT_ENVIO_EMAIL', 'CD_ETAPA', 'DS_ETAPA', 'DT_ETAPA', 'REGRA_PACOTE_1',
                            'REGRA_PACOTE_2', 'NR_DIAS_VENC_ATEND', 'CD_SITUACAO_GLOSA', 'IE_CONSISTE_SIT_GLOSA', 'IE_CLINICA', 'DT_BAIXA_ESCRITURAL',
                            'IE_CANCELAMENTO_CONTA', 'IE_STATUS_REPASSE', 'DT_CONTA_PROTOCOLO', 'VL_ESTORNO', 'IE_ESTORNO']

        df_producoes_medicas = pd.DataFrame(list_prod_medica, columns=header_prod_med)
        # Quando o médico executor foi trocado no momento do repasse, logo utiliza-se o código do médico de repasse para realizar previsão da regra de repasse.
        df_producoes_medicas['NVL_CD_MEDICO_EXEC_REPASSE'] = df_producoes_medicas['CD_MEDICO_EXECUTOR'].fillna(df_producoes_medicas['CD_MEDICO_REPASSE'])
        log_message(f'Total de Produções -> {len(df_producoes_medicas)}')

        log_message("Previsão de regra de repasse")
        df_procedimentos = execute_script_previsao_regra_repasse(df_producoes_medicas, connection)

        # Anonimização
        df_procedimentos['NM_PACIENTE_ANON'] = df_procedimentos['NM_PACIENTE'].apply(anonymize)
        df_procedimentos = df_procedimentos.drop("NM_PACIENTE", axis=1)

        
        # df_procedimentos['NR_SEQ_PROC_PACOTE'] = df_procedimentos['NR_SEQ_PROC_PACOTE'].astype('Int64') 
        # df_procedimentos['REGRA_PACOTE_1']     = df_procedimentos['REGRA_PACOTE_1'].fillna(0).astype('Int64')
        # df_procedimentos['REGRA_PACOTE_2']     = df_procedimentos['REGRA_PACOTE_2'].fillna(0).astype('Int64')
        df_procedimentos['IE_PERTENCE_PACOTE'] = df_procedimentos.apply(verifica_regra, axis=1)
                
        return df_procedimentos

def script2_obter_regras_repasse(connection):
       
    cursor = connection.cursor()            

    list_regras_repasse = []
    for result in cursor.execute(query.getRegrasRepasse()):
        list_regras_repasse.append(result)

    header_regra_repasse = [ #'CD_REGRA_NR_SEQ_CRITERIO_R', 
                            'CD_REGRA_R', 'NR_SEQ_CRITERIO_R', 'IE_FORMA_CALCULO_R', 'TX_ANESTESISTA_R',
                        'TX_MEDICO_R', 'TX_MATERIAIS_R', 'TX_AUXILIARES_R', 'TX_CUSTO_OPERACIONAL_R', 'VL_LIMITE_REGRA',
                        'IE_HONORARIO_R', 'TX_PROCEDIMENTO_R', 'IE_PERC_PACOTE_R', 'IE_REPASSE_CALC_R', 'IE_TIPO_ATEND_CALC_R', 'VL_REPASSE_R',
                        'NR_SEQ_REGRA_PRIOR_REPASSE_R', 'VL_MINIMO_R', 'IE_LIMITE_QTDADE_R', 'IE_CAMPO_BASE_VL_REPASSE_R', 'DS_REGRA',
                        'DS_OBSERVACAO_CRITERIO_R', 'IE_LIB_LAUDO_PROC_R', 'DT_VIGENCIA_INICIAL_R', 'DT_VIGENCIA_FINAL_R', 'DT_ATUALIZACAO_R']

    df_regras_repasse = pd.DataFrame(list_regras_repasse, columns=header_regra_repasse)

    cursor.close()

    log_message(f"Quantidade de regras de repasses: {len(df_regras_repasse)}")
    return df_regras_repasse

def script3_obter_forma_repasse(connection):
    
    cursor = connection.cursor()

    list_forma_repasse = []
    for result in cursor.execute(query.getFormaRepasse()):
        list_forma_repasse.append(result)
        
    header_forma_repasse = [ 'CD_REGRA_FR', 'NR_SEQ_ITEM_FR', 'NR_SEQ_CATEGORIA_FR', 'CD_PESSOA_FISICA_FR', 'TX_REPASSE_FR',
                            'NR_SEQ_TERCEIRO_FR', 'IE_BENEFICIARIO_FR', 'IE_FUNCAO_MEDICO_FR', 'IE_PERC_SALDO_FR', 'DT_FIM_VIGENCIA_FR',
                            'DT_ATUALIZACAO_FR', 'EXECUTOR_TERCEIRO_FR', 'IE_SITUACAO_FR']

    df_forma_repasse = pd.DataFrame(list_forma_repasse, columns=header_forma_repasse)

    cursor.close()
    
    log_message(f"Quantidade de formas de repasses: {len(df_forma_repasse)}")
    return df_forma_repasse


def calc_vl_especial_r(connection, forma_calculo_r_copy):
    
    cursor = connection.cursor()
    
    list_regras_repasse = []
    header_especial = ['NR_SEQ_CRITERIO_ESPECIAL', 'NR_SEQ_PROCED', 'VL_REPASSE_ESPECIAL']
    for index, row in forma_calculo_r_copy.iterrows():
        nr_seq_criterio = row['NR_SEQ_CRITERIO_PREVISTO']
        nr_seq_proced = row['NR_SEQ_PROCED']

        # Chamada da procedure e retorno da regra de repasse
        cols_input_regra = [nr_seq_criterio, nr_seq_proced]
        vl_repasse_especial = cursor.callfunc('tasy.obter_vl_repasse_adic', int, cols_input_regra)

        list_result = [nr_seq_criterio, nr_seq_proced, vl_repasse_especial]
        list_regras_repasse.append(list_result)

    df_regras_repasse = pd.DataFrame(list_regras_repasse, columns=header_especial)

    cursor.close()
   
    return df_regras_repasse


def valor_repasse_faturado(row):
    """
    Função para calcular o valor de repasse faturado.
    Se o valor de repasse for nulo, retorna 0.
    Caso contrário, retorna o valor de repasse.
    """
    if pd.isnull(row['VL_REPASSE']):
        return 0
    else:
        return row['VL_REPASSE']

def set_medico_forma_repasse(row):
    if pd.isnull(row['IE_BENEFICIARIO_FR']):
        return row['CD_MEDICO_EXECUTOR']
    elif row['IE_BENEFICIARIO_FR'] == 'E':
        return row['CD_MEDICO_EXECUTOR']
    else:
        return row['CD_PESSOA_FISICA_FR'] if not pd.isnull(row['CD_PESSOA_FISICA_FR']) else row['CD_MEDICO_EXECUTOR']

def upload_zip_to_s3(dir_file_zip, dir_s3_zip):
    
    log_message("Enviando ZIP para S3")
    upload_to_s3(dir_file_zip, v_bucket_s3, dir_s3_zip)
    log_message(f"Arquivos csv enviados para AWS {dir_s3_zip}")

def chunk_data(data, chunk_size):
    """Divide os dados em lotes de até chunk_size registros."""
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

def log_message(msg, nivel='info'):
    print(msg)
    if nivel == 'info':
        logging.info(msg)
    elif nivel == 'error':
        logging.error(msg, exc_info=True)
        sys.exit(1)

def configure_exec():
    
    global output
    output, logs = create_directory()

    # Obter a data e hora atual
    dt_inicio_exec_carga = datetime.now()
    global dt_execucao_carga_str
    dt_execucao_carga_str = dt_inicio_exec_carga.strftime('%Y-%m-%d %H:%M:%S')
    
    # Diretório do S3
    global dir_s3_date
    dir_s3_date = dt_inicio_exec_carga.strftime('%Y%m%d')
    
    global date_file
    date_file = dt_inicio_exec_carga.strftime('%Y%m%d_%H%M%S')#.strftime('%Y_%m_%d_%H_%M_%S')
    # Configuração de logging
    filename_log=f'log_{date_file}.log'
    dir_file_log = f'{logs}/{filename_log}' 
    logging.basicConfig(filename=dir_file_log, level=logging.INFO,
                        format='%(asctime)s - %(name)s -  %(levelname)s - %(message)s')
    
    global pool
    pool = configure_pool()
    
    try:
        if file_exists(v_file_last_load):
            global ult_carga
            global data_fim_carga
            global data_inicio_carga
            global tipo_carga
            global data_inicio_carga_glosa
            ult_carga = read_file_load(v_file_last_load)

            # tempo de atraso
            data_inicio_carga_aux = datetime.strptime(ult_carga, '%Y-%m-%d %H:%M:%S') - timedelta(minutes=5)
            ult_carga_c_atraso = data_inicio_carga_aux.strftime('%Y-%m-%d %H:%M:%S')

            data_inicio_carga = ult_carga_c_atraso #ult_carga
            data_fim_carga = dt_execucao_carga_str
            tipo_carga = 'incr'
            
            data_inicio_carga_glosa = '2025-10-01 00:00:00'

            log_message(f"Data de Última Carga  : {ult_carga}")
            log_message(f"Data de Última Carga c Atraso de 5 min: {ult_carga_c_atraso}")

            process_data(dt_inicio_exec_carga, data_inicio_carga, data_fim_carga, tipo_carga)
        else:
            data_inicio_carga = '2025-10-01 00:00:00'
            data_inicio_carga_glosa = data_inicio_carga
            data_fim_carga = dt_execucao_carga_str#'2025-04-10 23:59:59'
            tipo_carga = 'full'

            process_data(dt_inicio_exec_carga, data_inicio_carga, data_fim_carga, tipo_carga)

    except FileNotFoundError as e:
        log_message(f"Arquivo de última carga não encontrado : {e}", nivel='error')
    
def process_data(dt_inicio_exec_carga, data_inicio_carga, data_fim_carga, tipo_carga):
    """
    Função para processar as etapas do ETL.
    """
        
    try:
        log_message(f"\nIniciando processo ETL")

        log_message(f"Tipo de carga: {tipo_carga}")
        log_message(f"Data de Inicio do processamento  : {data_inicio_carga}")
        log_message(f"Data de Fim do processamento  : {data_fim_carga}")

        connection = pool.acquire()

        # 0: Obter código do médico
        log_message('\n0: Gerar a tabela de dados médicos')
        medicos = get_medicos_api()
        
        
        list_cpf_medicos = []
        for medico in medicos[:2]:
            list_cpf_medicos.append(medico['cpf'])
        
        #loop = 0
        incr_loop = 0

        # 2: Gerar a tabela de regras e repasses com seus valores.
        log_message('\n2: Gerar a tabela de regras de repasses')
        tbl_regras_repasse = script2_obter_regras_repasse(connection)
        #tbl_regras_repasse['CD_REGRA_R'] = tbl_regras_repasse['CD_REGRA_R'].round(0).astype('Int64')
        #tbl_regras_repasse['NR_SEQ_CRITERIO_R'] = tbl_regras_repasse['NR_SEQ_CRITERIO_R'].round(0).astype('Int64')

        # 3: Obter regras de repasse para cada procedimento
        log_message('\n3: Gerar a tabela de formas de repasses')
        tbl_forma_repasse = script3_obter_forma_repasse(connection)
        formas_repasse_ativas = tbl_forma_repasse['IE_SITUACAO_FR'] == 'ATIVO'
        df_forma_repasse = tbl_forma_repasse[formas_repasse_ativas].copy()

        # Loop para percorrer o array de 50 em 50 posições
        total_medicos = len(list_cpf_medicos)
        tamanho_lote = 50
        for i in range(0, total_medicos, tamanho_lote):
            incr_loop += 1
            loop = str(incr_loop).zfill(2)
            medicos_selecionados = list_cpf_medicos[i:i+tamanho_lote]
            
            log_message(f"***FAZER MELHORIA. TIRAR DO LOOP E CRIAR UM MAPPING***\nProcessando {i + len(medicos_selecionados)}/{len(list_cpf_medicos)} médicos")
            tbl_dados_medicos = script0_obter_cd_medico(connection, medicos_selecionados)
            
            df_cd_pessoa_fisica = tbl_dados_medicos[['CD_PESSOA_FISICA']]
            print(f"Quantidade de médicos encontrados no período informado: {len(df_cd_pessoa_fisica)}")
            
            log_message('\n1: Gerar a tabela de produção médica')
            tbl_prod_medica = script1_obter_prod_medica(connection, df_cd_pessoa_fisica)
            if tbl_prod_medica.empty:
                log_message(f"Sem produção médica para os médicos do bloco {medicos_selecionados} do {loop}.")
                continue
            
          
            tbl_prod_medica_cpf = pd.merge(tbl_prod_medica, tbl_dados_medicos,  
                                    left_on=[ "CD_MEDICO_EXECUTOR"],
                                    right_on=["CD_PESSOA_FISICA"], how='left')
            tbl_prod_medica_cpf.drop(['CD_PESSOA_FISICA', 'NM_MEDICO'], axis=1, inplace=True)
            
            #tbl_prod_medica['CD_REGRA_PREVISTO'] = (tbl_prod_medica['CD_REGRA_PREVISTO']).round(0).astype('Int64')
            #tbl_prod_medica['NR_SEQ_CRITERIO_PREVISTO'] = (tbl_prod_medica['NR_SEQ_CRITERIO_PREVISTO']).round(0).astype('Int64')
            df_prod_regra = pd.merge(tbl_prod_medica_cpf, tbl_regras_repasse,
                                    left_on=[ "CD_REGRA_PREVISTO", "NR_SEQ_CRITERIO_PREVISTO"],
                                    right_on=["CD_REGRA_R", "NR_SEQ_CRITERIO_R"], how='left')
            df_prod_regra.drop(['CD_REGRA_R', 'NR_SEQ_CRITERIO_R'], axis=1, inplace=True)


            df_forma_repasse_terceiro = pd.merge(df_prod_regra, df_forma_repasse,
                                                    left_on=["CD_REGRA_PREVISTO"],
                                                    right_on=["CD_REGRA_FR"], how='left')
            df_forma_repasse_terceiro.drop(['CD_REGRA_FR'], axis=1, inplace=True)

            # IDENTIFICAR ITENS A FATURAR QUE POSSUEM REGRA COM FORMA DE REPASSE ESPECIAL - NOTA: ESSA CHAMA UMA NOVA PROCEDURE NO BANCO
            terceiro_sregra_especial = df_forma_repasse_terceiro[df_forma_repasse_terceiro['IE_FORMA_CALCULO_R'] != 'R'].copy()
            terceiro_cregra_especial = df_forma_repasse_terceiro[df_forma_repasse_terceiro['IE_FORMA_CALCULO_R'] == 'R'].copy()

            # ITENS COM REGRA ESPECIAL ENTÃO CHAMAR FUNÇÃO
            if not terceiro_cregra_especial.empty:
                log_message(f"{len(terceiro_cregra_especial)} TEM REGRA ESPECIAL DE REPASSE.")
                if (terceiro_cregra_especial['IE_FORMA_CALCULO_R'] == 'R').any():
                    log_message("FORMA_CALCULO = R")
                    #forma_calculo_r_copy = terceiro_cregra_especial[terceiro_cregra_especial['IE_FORMA_CALCULO_R'] == 'R'].copy()
                    result_forma_calculo_r = calc_vl_especial_r(connection, terceiro_cregra_especial)
                    forma_calculo_r = pd.merge(terceiro_cregra_especial, result_forma_calculo_r,  
                                                    left_on=["NR_SEQ_CRITERIO_PREVISTO", "NR_SEQ_PROCED"],
                                                    right_on=["NR_SEQ_CRITERIO_ESPECIAL", "NR_SEQ_PROCED"], how='left')
                    
                    #VL_REPASSE_ESPECIAL = forma_calculo_r['VL_REPASSE_ESPECIAL']
                    #forma_calculo_r.loc[:, 'VL_REPASSE_PREVISTO'] = (forma_calculo_r['VL_REPASSE_ESPECIAL'] * forma_calculo_r['QT_PROCEDIMENTO']) * (forma_calculo_r['TX_REPASSE_FR'] / 100)
                    #log_message(len(forma_calculo_r))

            if not terceiro_sregra_especial.empty:
                log_message(f"{len(terceiro_sregra_especial)} NAO TEM REGRA ESPECIAL DE REPASSE.")

                if (terceiro_sregra_especial['IE_FORMA_CALCULO_R'] == 'P').any():
                    log_message("FORMA_CALCULO = P")
                    # forma_calculo_p = terceiro_sregra_especial[terceiro_sregra_especial['IE_FORMA_CALCULO_R'] == 'P'].copy()
                    # forma_calculo_p['TX_MEDICO_R'] = forma_calculo_p['TX_MEDICO_R'].fillna(0).astype(int)

                    # forma_calculo_p['VL_REPASSE_PREVISTO'] = forma_calculo_p.apply(calcular_forma_repasse_p, axis=1)

                    # log_message(len(forma_calculo_p))

                if (terceiro_sregra_especial['IE_FORMA_CALCULO_R'] == 'V').any():
                    log_message("FORMA_CALCULO = V")
                    # forma_calculo_v = terceiro_sregra_especial[terceiro_sregra_especial['IE_FORMA_CALCULO_R'] == 'V'].copy()

                    # forma_calculo_v['VL_REPASSE_PREVISTO'] = forma_calculo_v.apply(calcular_forma_repasse_v, axis=1)

                    # log_message(len(forma_calculo_v))

                if (terceiro_sregra_especial['IE_FORMA_CALCULO_R'] == 'K').any():
                    log_message("FORMA_CALCULO = K")
                    # forma_calculo_k = terceiro_sregra_especial[terceiro_sregra_especial['IE_FORMA_CALCULO_R'] == 'K'].copy()

                    # forma_calculo_k['VL_REPASSE_PREVISTO'] = forma_calculo_k.apply(calcular_forma_repasse_k, axis=1)

                    # log_message(len(forma_calculo_k))

                if (terceiro_sregra_especial['IE_FORMA_CALCULO_R'] == 'U').any():
                    log_message("FORMA_CALCULO = U")
                    # forma_calculo_u = terceiro_sregra_especial[terceiro_sregra_especial['IE_FORMA_CALCULO_R'] == 'U'].copy()

                    # forma_calculo_u['VL_REPASSE_PREVISTO'] = forma_calculo_u.apply(calcular_forma_repasse_u, axis=1)

                    # log_message(len(forma_calculo_u))

                if terceiro_sregra_especial['IE_FORMA_CALCULO_R'].isnull().any():
                    log_message("FORMA_CALCULO = NULL")
                    # forma_calculo_null = terceiro_sregra_especial[terceiro_sregra_especial['IE_FORMA_CALCULO_R'].isnull()].copy()

                    # forma_calculo_null.loc[:, 'VL_REPASSE_PREVISTO'] = 0

                    # log_message(len(forma_calculo_null))

                #terceiro_sregra_especial['VL_REPASSE_PREVISTO'] = None
                terceiro_sregra_especial['NR_SEQ_CRITERIO_ESPECIAL'] = None
                terceiro_sregra_especial['VL_REPASSE_ESPECIAL'] = None
                

          


            date_file_formated = f'{date_file}_part{loop}'            
            save_data(terceiro_sregra_especial, date_file_formated, ambiente)

            dir_s3_zip = f'raw/{dir_s3_date}/{date_file_formated}.csv'
            output_filename_csv = f'{output}\{date_file_formated}.csv'
            print(f"Arquivo local: {output_filename_csv}")
            print(f"Enviando arquivo para S3: {dir_s3_zip}")
            print(f"Bucket: {v_bucket_s3}")
            upload_to_s3(output_filename_csv, v_bucket_s3, dir_s3_zip)
            # 8: Exluir arquivos gerados
            #excluir_arquivos_em_diretorio(dir_csv, pasta=True)
            #excluir_arquivos_em_diretorio(dir_zip, pasta=True)
        pool.release(connection)
        pool.close()
                
        log_message(f"Atualizando arquivo de última carga: {v_file_last_load}")
        # write_file_load(v_file_last_load, data_fim_carga)

        log_message("* * * Tempo de Processamento * * *")
        dt_fim_exec_carga = datetime.now()
        minutos_exec =  dt_fim_exec_carga - dt_inicio_exec_carga

        log_message(f"Inicio: {dt_inicio_exec_carga.strftime('%Y-%m-%d %H:%M:%S')}")
        log_message(f"Fim: {dt_fim_exec_carga.strftime('%Y-%m-%d %H:%M:%S')}")
        log_message(f"Tempo de Duração : {minutos_exec}")
        log_message("* * * * * * * * * * * * * * * * * *")

        log_message('\nConcluído com Sucesso')
    except Exception as e:
        log_message(f"Erro no processo ETL: {str(e)}", nivel='error')

def get_medicos_api():
    
    headers = {
        "x-api-key": v_api_key
    }

    # Constrói a API_URL completa no seu código
    if v_instituicao_saude and v_api_base_url:
        api_url = f"{v_api_base_url}/{v_instituicao_saude}/profissionais-saude"
        
    else:
        log_message(f"Erro: lista de médicos não encontrada", nivel='error')

    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        log_message("Profissionais de saúde recebidos com sucesso")
        list_cpf_medicos = []
        for profissional in data:
            list_cpf_medicos.append(profissional)
    elif response.status_code == 401:
        log_message(f"Erro de autenticação (401 Unauthorized). Verifique sua x-api-key.", nivel='error')
    else:
        log_message(f"Erro ao acessar a API. Código de status: {response.status_code}", nivel='error')
        log_message(f"Mensagem de erro: {response.text}", nivel='error')
    
    return list_cpf_medicos

if __name__ == "__main__":

    if getattr(sys, 'frozen', False):
        base_path = sys._MEIPASS
    else:
        base_path = os.path.dirname(__file__)
    
    env_file_path = os.path.join(base_path, f".env")

    # Carregar as variáveis de ambiente do arquivo .env
    load_dotenv(env_file_path)
    
    # Identificar o ambiente
    ambiente = os.getenv("APP_ENV")
    
    print(f"\n\nCarregando variáveis de : {env_file_path}")
    print(f"Ambiente: {ambiente}\n")

    # Selecionar variáveis corretas com base no ambiente
    global v_file_last_load 
    v_file_last_load = os.getenv("FILE_LAST_LOAD")

    global v_chunck_size
    v_chunck_size = os.getenv("CHUNK_SIZE")
    v_chunck_size = int(v_chunck_size)
    v_instituicao_saude = os.getenv("ID_INSTITUICAO_SAUDE")

    if ambiente == "prod":
        v_bucket_s3 = os.getenv("PROD_BUCKET_S3")
        db_dsn = os.getenv("PROD_DB_DSN")
        db_user = os.getenv("PROD_DB_USER")
        db_pass = os.getenv("PROD_DB_PASS")
        v_api_base_url = os.getenv('PROD_API_URL')
        v_api_key = os.getenv('PROD_API_KEY')
    elif ambiente == "dev":
        v_bucket_s3 = os.getenv("DEV_BUCKET_S3")
        db_dsn = os.getenv("DEV_DB_DSN")
        db_user = os.getenv("DEV_DB_USER")
        db_pass = os.getenv("DEV_DB_PASS")
        v_api_base_url = os.getenv('DEV_API_URL')
        v_api_key = os.getenv('DEV_API_KEY')
    else:
        v_bucket_s3 = os.getenv("TEST_BUCKET_S3")
        db_dsn = os.getenv("TEST_DB_DSN")
        db_user = os.getenv("TEST_DB_USER")
        db_pass = os.getenv("TEST_DB_PASS")
        v_api_base_url = os.getenv('TEST_API_URL')
        v_api_key = os.getenv('TEST_API_KEY')

    configure_exec()