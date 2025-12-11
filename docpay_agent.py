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

pd.set_option('display.max_columns', None)

def create_directory(dir_files):

    # if getattr(sys, 'frozen', False):
    #     base_path = sys._MEIPASS
    # else:
    #     base_path = os.path.dirname(__file__)

    global dir_zip
    dir_zip = f'{base_path}\{dir_files}\zip'
    global dir_csv
    dir_csv = f'{base_path}\{dir_files}\csv'
    global dir_logs
    dir_logs = 'logs'

    if os.path.exists(dir_files) and os.path.isdir(dir_files):
        print(f"dir_zip: {dir_zip}")
        print(f"dir_csv: {dir_csv}")
        print(f"dir_logs: {dir_logs}")
    else:
        os.makedirs(dir_logs, exist_ok=True)
        os.makedirs(dir_csv, exist_ok=True)
        os.makedirs(dir_zip, exist_ok=True)
        print(f"Criando diretórios em: {dir_files}")

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

def save_to_csv(data, filename):
    data.to_csv(filename, index=False, sep='|', encoding='UTF-16')
    log_message(f"Arquivo CSV criado com sucesso: {filename}")

def upload_to_s3(filename, bucket, s3_key):
    s3 = boto3.client('s3')
    s3.upload_file(filename, bucket, s3_key)

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
        #for result in cursor.execute(query.getAllProdMedica(), cd_medico_executor=list_cd_pessoa_fisica_medicos, dt_inicio_carga=data_inicio_carga, dt_fim_carga=data_fim_carga):    
        for result in cursor.execute(query.getAllProdMedica(data_inicio_carga, data_fim_carga, list_cd_pessoa_fisica_medicos)):
            list_prod_medica.append(result)
    else :
        for result in cursor.execute(query.getAllProdMedica_incr(data_inicio_carga, data_fim_carga,list_cd_pessoa_fisica_medicos)):
            list_prod_medica.append(result)
    
    cursor.close()
    return list_prod_medica

def execute_script_previsao_regra_repasse(df_repasse_s_regra, connection):
    cursor = connection.cursor()

    OUTPUT_CD_REGRA_P = cursor.var(int)  # (cx_Oracle.NUMBER)
    OUTPUT_NR_SEQ_CRITERIO_P = cursor.var(int)  # (cx_Oracle.NUMBER)

    loop_proc = 0
    list_previsao = []
    if not df_repasse_s_regra.empty:

        for index, row in df_repasse_s_regra.iterrows():
            CD_CONVENIO_PARAMETRO = int(row['CD_CONVENIO_PARAMETRO'])
            CD_EDICAO_AMB = int(row['CD_EDICAO_AMB'])
            CD_ESTABELECIMENTO = int(row['CD_ESTABELECIMENTO'])
            CD_MEDICO_RESP = int(row['CD_MEDICO_RESP'])
            CD_MEDICO_EXEC_P = row['NVL_CD_MEDICO_EXEC_REPASSE']
            CD_CGC_PRESTADOR = row['CD_CGC_PRESTADOR']
            IE_FUNCAO_MEDICO = row['IE_FUNCAO_MEDICO']
            IE_PARTICIPOU_SUS = row['IE_PARTICIPOU_SUS']
            IE_RESPONSAVEL_CREDITO = row['IE_RESPONSAVEL_CREDITO']
            IE_TIPO_ATENDIMENTO = int(row['IE_TIPO_ATENDIMENTO'])
            IE_TIPO_ATO_SUS_P = row['IE_TIPO_ATO_SUS']
            IE_TIPO_SERVICO_SUS_P = row['IE_TIPO_SERVICO_SUS']
            NM_USUARIO_ORIGINAL = row['NM_USUARIO_ORIGINAL']
            NR_SEQ_ETAPA_CHECKUP = int(row['NR_SEQ_ETAPA_CHECKUP'])
            NR_SEQ_PARTIC = row['NR_SEQ_PARTIC']
            NR_SEQ_PROCED = int(row['NR_SEQ_PROCED'])

            cols_input_regra = [CD_CONVENIO_PARAMETRO,
                                CD_EDICAO_AMB,
                                CD_ESTABELECIMENTO,
                                CD_MEDICO_RESP,
                                CD_MEDICO_EXEC_P,
                                CD_CGC_PRESTADOR,
                                IE_FUNCAO_MEDICO,
                                IE_PARTICIPOU_SUS,
                                IE_RESPONSAVEL_CREDITO,
                                IE_TIPO_ATENDIMENTO,
                                IE_TIPO_ATO_SUS_P,
                                IE_TIPO_SERVICO_SUS_P,
                                NM_USUARIO_ORIGINAL,
                                NR_SEQ_ETAPA_CHECKUP,
                                NR_SEQ_PARTIC,
                                NR_SEQ_PROCED,
                                OUTPUT_CD_REGRA_P,
                                OUTPUT_NR_SEQ_CRITERIO_P
                                ]
            
            # Chamada da procedure e retorno da regra de repasse
            cursor.callproc('tasy.obter_regra_proc_repasse', cols_input_regra)
            list_result_func = [NR_SEQ_PROCED,
                           NR_SEQ_PARTIC,
                           OUTPUT_CD_REGRA_P.getvalue(),
                           OUTPUT_NR_SEQ_CRITERIO_P.getvalue()]
            list_previsao.append(list_result_func)

            loop_proc += 1
            if loop_proc % 1000 == 0:
                print(f"Processados {loop_proc} procedimentos para previsão de regra de repasse.")

                
    # PREVISÃO DE ITENS SEM REGRA
    header_sregra = ['NR_SEQ_PROCED', 'NR_SEQ_PARTIC', 'CD_REGRA_PREVISTO', 'NR_SEQ_CRITERIO_PREVISTO']
    df_previsao = pd.DataFrame(list_previsao, columns = header_sregra)

    # Join para juntar resultado da previsão com df sem repasse.
    df_previsao_regra = df_repasse_s_regra.merge(df_previsao, on=['NR_SEQ_PROCED', 'NR_SEQ_PARTIC'], how='left')


    cursor.close()
    return df_previsao_regra

def verifica_regra(row):
    if pd.isnull(row['NR_SEQ_PROC_PACOTE']):
        return 'N'
    elif row['NR_SEQ_PROC_PACOTE'] != row['NR_SEQ_PROCED'] and row['REGRA_PACOTE_1'] == 1:
        return 'N'
    elif row['NR_SEQ_PROC_PACOTE'] == row['NR_SEQ_PROCED'] and row['REGRA_PACOTE_2'] == 1:
        return 'N'
    else:
        return 'S'

def script0_obter_cd_medico(connection, list_cpf_medicos):


    list_cpf = tuple(list_cpf_medicos)
    
    cursor = connection.cursor()

    list_pessoa = []
    for result in cursor.execute(query.getCDMedico(list_cpf)):
        list_pessoa.append(result)

    header_pessoa = ['CD_PESSOA_FISICA', 'NM_MEDICO', 'NR_CPF']
    tbl_dados_medico = pd.DataFrame(list_pessoa, columns=header_pessoa)
    
    cursor.close()

    return tbl_dados_medico

def script1_obter_prod_medica(connection, df_cd_pessoa_fisica):

    list_cd_pessoa_fisica_medicos = tuple(df_cd_pessoa_fisica['CD_PESSOA_FISICA'].values)
  
    list_prod_medica = execute_script_prod_medica(connection, list_cd_pessoa_fisica_medicos)

    if not list_prod_medica:
        log_message("Sem novas produções. Encerrando o programa.")
        log_message(f"Atualizando arquivo de última carga: {v_file_last_load}")
        write_file_load(v_file_last_load, data_fim_carga)
        sys.exit(1)  # Indica erro ao sair

    # Transformando em DF's todos itens c regra
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

    #alteração no tipo de dado das colunas que vão para procedure
    df_producoes_medicas['CD_CONVENIO_PARAMETRO'] = df_producoes_medicas['CD_CONVENIO_PARAMETRO'].round(0)
    df_producoes_medicas['CD_EDICAO_AMB'] = df_producoes_medicas['CD_EDICAO_AMB'].round(0)
    df_producoes_medicas['CD_ESTABELECIMENTO'] = df_producoes_medicas['CD_ESTABELECIMENTO'].round(0)
    df_producoes_medicas['CD_MEDICO_RESP'] = df_producoes_medicas['CD_MEDICO_RESP'].round(0)
    df_producoes_medicas['CD_CGC_PRESTADOR'] = df_producoes_medicas['CD_CGC_PRESTADOR']
    df_producoes_medicas['IE_FUNCAO_MEDICO'] = df_producoes_medicas['IE_FUNCAO_MEDICO']
    df_producoes_medicas['IE_PARTICIPOU_SUS'] = df_producoes_medicas['IE_PARTICIPOU_SUS']
    df_producoes_medicas['IE_RESPONSAVEL_CREDITO'] = df_producoes_medicas['IE_RESPONSAVEL_CREDITO']
    df_producoes_medicas['IE_TIPO_ATENDIMENTO'] = df_producoes_medicas['IE_TIPO_ATENDIMENTO'].round(0)
    df_producoes_medicas['IE_TIPO_ATO_SUS'] = df_producoes_medicas['IE_TIPO_ATO_SUS'].round(0)
    df_producoes_medicas['IE_TIPO_SERVICO_SUS'] = df_producoes_medicas['IE_TIPO_SERVICO_SUS'].round(0)
    df_producoes_medicas['NM_USUARIO_ORIGINAL'] = df_producoes_medicas['NM_USUARIO_ORIGINAL']
    df_producoes_medicas['NR_SEQ_ETAPA_CHECKUP'] = df_producoes_medicas['NR_SEQ_ETAPA_CHECKUP'].round(0)
    df_producoes_medicas['NR_SEQ_PARTIC'] = df_producoes_medicas['NR_SEQ_PARTIC'].round(0)
    df_producoes_medicas['NR_SEQ_PROCED'] = df_producoes_medicas['NR_SEQ_PROCED'].round(0)
    df_producoes_medicas['NVL_CD_MEDICO_EXEC_REPASSE'] = df_producoes_medicas['CD_MEDICO_EXECUTOR'].fillna(df_producoes_medicas['CD_MEDICO_REPASSE'])

    log_message(f'Total de Produções -> {len(df_producoes_medicas)}')

    #Retorna o DF com todas as previsoes
    log_message("Iniciando execução da procedure de previsão de regra de repasse")
    df_repasse_prev_regra = execute_script_previsao_regra_repasse(df_producoes_medicas, connection)

    # Concat dos dfs c/ regra e s/ regra
    #df_procedimentos = pd.concat([df_repasse_c_regra, df_repasse_prev_regra], axis=0,  ignore_index=True)
    df_procedimentos = df_repasse_prev_regra.copy()
    # Anonimização
    df_procedimentos['NM_PACIENTE_ANON'] = df_procedimentos['NM_PACIENTE'].apply(anonymize)
    df_procedimentos = df_procedimentos.drop("NM_PACIENTE", axis=1)

    # Itens que nçao pertencem a pacote.
    df_procedimentos['IE_PERTENCE_PACOTE'] = df_procedimentos.apply(verifica_regra, axis=1)
    df_procedimentos_validos = df_procedimentos[df_procedimentos['IE_PERTENCE_PACOTE'] == 'N'].copy()

    
    log_message(f"Quantidade de produção médica Válida:  {len(df_procedimentos_validos)}")

    #df_procedimentos_validos.to_csv(f"{dir_csv}\prod_medica_s_pacotes_{date_file}.csv", index=False, sep='|', encoding='UTF-16')
    #df_procedimentos.to_csv(f"{dir_csv}\prod_medica_c_pacotes_{date_file}.csv", index=False, sep='|', encoding='UTF-16')

    return df_procedimentos_validos

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

def script4_obter_glosa(connection):
   
    cursor = connection.cursor()

    list_glosas = []
    
    log_message(f"Data Glosa : {data_inicio_carga_glosa}")
    for result in cursor.execute(query.getGlosa(), dt_inicio_carga=data_inicio_carga_glosa, dt_fim_carga=data_fim_carga):
        list_glosas.append(result)

    # Caso queira implementar glosa incremental, descomentar abaixo
    # if (tipo_carga == 'full') :
    #     #dt_glosa = datetime.now() - timedelta(days=120)
    #     #data_inicio_carga = '2024-01-01 00:00:00' #dt_glosa.strftime('%Y-%m-%d 00:00:00')
    #     log_message(f"Data Glosa : {data_inicio_carga}")
    #     for result in cursor.execute(query.getGlosa(), dt_inicio_carga=data_inicio_carga, dt_fim_carga=data_fim_carga):
    #         list_glosas.append(result)
    # else :
    #     log_message("**** IMPLEMENTAR SCRIPT DE GLOSA INCREMENTAL*** \nbeginning execute incremental load")
    #     for result in cursor.execute(query.getGlosaIncr(), dt_inicio_carga=data_inicio_carga, dt_fim_carga=data_fim_carga):
    #         list_glosas.append(result)
    
    header_glosa = ['ID_ORIGEM_GLOSA', 'ORIGEM_GLOSA', 'NR_SEQ_PROCED_GLOSA', 'NR_INTERNO_CONTA_GLOSA', 'NR_SEQ_PARTIC_GLOSA',
                     'IE_GLOSA', 'VL_COBRADO', 'VL_GLOSA', 'DS_TIPO_AJUSTE_GLOSA', 'DT_REF_GLOSA', 
                     'IE_TIPO_AUDITORIA', 'DS_TIPO_AUDITORIA', 'IE_TIPO_AUDITORIA_ITEM', 'DS_MOTIVO_GLOSA']


    df_glosa = pd.DataFrame(list_glosas, columns=header_glosa)

    cursor.close()

    log_message(f"Quantidade de glosas: {len(df_glosa)}")
    return df_glosa

def gerar_base_final_c_valor_repasse(dt_execucao_carga_str, connection, tbl_prod_medica, tbl_regras_repasse, tbl_forma_repasse, tbl_dados_medicos):

    # Convertendo para tipo inteiro para realizar merge 
    tbl_prod_medica['CD_REGRA_PREVISTO'] = (tbl_prod_medica['CD_REGRA_PREVISTO']).round(0).astype('Int64')
    tbl_prod_medica['NR_SEQ_CRITERIO_PREVISTO'] = (tbl_prod_medica['NR_SEQ_CRITERIO_PREVISTO']).round(0).astype('Int64')
    tbl_regras_repasse['CD_REGRA_R'] = tbl_regras_repasse['CD_REGRA_R'].round(0).astype('Int64')
    tbl_regras_repasse['NR_SEQ_CRITERIO_R'] = tbl_regras_repasse['NR_SEQ_CRITERIO_R'].round(0).astype('Int64')

    # Produção médica com informações de regras de repasse
    df_prod_regra = pd.merge(tbl_prod_medica, tbl_regras_repasse,
                             left_on=[ "CD_REGRA_PREVISTO", "NR_SEQ_CRITERIO_PREVISTO"],
                             right_on=["CD_REGRA_R", "NR_SEQ_CRITERIO_R"], how='left')
    # Calcular repasse
    ###### TALVEZZ APLICAR O APPLY NESSA FUNÇÃO
    df_vl_repasse_previsto = calc_vl_repasse_all(connection, df_prod_regra, tbl_forma_repasse)
   
    df_vl_repasse_previsto['CD_MEDICO_FORMA_REPASSE'] = df_vl_repasse_previsto.apply(set_medico_forma_repasse, axis=1)
    #df_prod_valor_repasse['VL_REPASSE_FINAL'] = df_prod_valor_repasse.apply(valor_repasse_final, axis=1)

    # Dados Médicos
    df_final = pd.merge(df_vl_repasse_previsto, tbl_dados_medicos,
                        left_on=["CD_MEDICO_FORMA_REPASSE"],
                        right_on=["CD_PESSOA_FISICA"], how='left')
    
    df_final['CD_MEDICO_FINAL'] = df_final['CD_MEDICO_FORMA_REPASSE'].fillna(df_final['CD_MEDICO_EXECUTOR'])
    df_final['NM_MEDICO_FINAL'] = df_final['NM_MEDICO'].fillna(df_final['NM_MEDICO_EXEC'])
    df_final['NR_CPF'] = df_final['NR_CPF'].astype(str).str.zfill(11)
    df_final['DT_INGESTAO'] = dt_execucao_carga_str
   
    df_final['VL_REPASSE_PREVISTO'] = df_final['VL_REPASSE_PREVISTO'].round(2).astype('Float64')
    df_final['CD_REGRA_REPASSE'] = df_final['CD_REGRA_REPASSE'].round(0).astype('Int64')
    df_final['NR_SEQ_CRITERIO_REPASSE'] = df_final['NR_SEQ_CRITERIO_REPASSE'].round(0).astype('Int64')
    df_final['NR_PRESCRICAO'] = df_final['NR_PRESCRICAO'].round(0).astype('Int64')
    df_final['NR_CIRURGIA'] = df_final['NR_CIRURGIA'].round(0).astype('Int64')
    df_final['NR_SEQ_PROC_INTERNO'] = df_final['NR_SEQ_PROC_INTERNO'].round(0).astype('Int64')
    df_final['CD_TIPO_PROCEDIMENTO'] = df_final['CD_TIPO_PROCEDIMENTO'].round(0).astype('Int64')
    df_final['REGRA_PACOTE_1'] = df_final['REGRA_PACOTE_1'].round(0).astype('Int64')
    df_final['REGRA_PACOTE_2'] = df_final['REGRA_PACOTE_2'].round(0).astype('Int64')
    df_final['NR_SEQ_ITEM_FR'] = df_final['NR_SEQ_ITEM_FR'].round(0).astype('Int64')
    #df_forma_repasse_terceiro = df_forma_repasse_terceiro.drop(columns=['CD_REGRA'], inplace=True) 
    df_final['ID_INSTITUICAO_SAUDE'] = v_instituicao_saude
    
    # Drop de colunas que não serão utilizadas, ja temos CD_REGRA e NR_SEQ_CRITERIO na base.
    df_final.drop(columns=['CD_REGRA_FR', 'CD_REGRA_R' , 'NR_SEQ_CRITERIO_R', 'NM_MEDICO'], axis=1, inplace=True)

    return df_final

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

def calcular_forma_repasse_p(row):
    if row['TX_PROCEDIMENTO_R'] != 0:
        return (
                (
                 (row['VL_PROCEDIMENTO'] * (row['TX_PROCEDIMENTO_R'] / 100)) #+
                #  (row['VL_ANESTESISTA'] * row['TX_ANESTESISTA_R'] / 100) +
                #  (row['VL_MATERIAIS'] * row['TX_MATERIAIS_R'] / 100) +
                #  (row['VL_AUXILIARES'] * row['TX_AUXILIARES_R'] / 100) +
                #  (row['VL_CUSTO_OPERACIONAL'] * row['TX_CUSTO_OPERACIONAL_R'] / 100)
                 ) * (row['TX_REPASSE_FR'] / 100)
                )
    elif row['VL_PARTICIPANTE'] > 0:
            return (((row['VL_PARTICIPANTE'] * row['TX_MEDICO_R'] / 100) +
                 (row['VL_ANESTESISTA'] * row['TX_ANESTESISTA_R'] / 100) +
                 (row['VL_MATERIAIS'] * row['TX_MATERIAIS_R'] / 100) +
                 (row['VL_AUXILIARES'] * row['TX_AUXILIARES_R'] / 100) +
                 (row['VL_CUSTO_OPERACIONAL'] * row['TX_CUSTO_OPERACIONAL_R'] / 100)
                ) * (row['TX_REPASSE_FR'] / 100))
    else :
            return (((row['VL_MEDICO'] * row['TX_MEDICO_R'] / 100) +
                     (row['VL_ANESTESISTA'] * row['TX_ANESTESISTA_R'] / 100) +
                     (row['VL_MATERIAIS'] * row['TX_MATERIAIS_R'] / 100) +
                     (row['VL_AUXILIARES'] * row['TX_AUXILIARES_R'] / 100) +
                     (row['VL_CUSTO_OPERACIONAL'] * row['TX_CUSTO_OPERACIONAL_R'] / 100)
                     ) * (row['TX_REPASSE_FR'] / 100))
    
def calcular_forma_repasse_v(row):
    # forma_calculo_v.loc[:, 'VL_REPASSE_PREVISTO'] = (forma_calculo_v['VL_REPASSE_R'] * forma_calculo_v['QT_PROCEDIMENTO']) * (forma_calculo_v['TX_REPASSE_FR'] / 100)
    return (row['VL_REPASSE_R'] * row['QT_PROCEDIMENTO']) * (row['TX_REPASSE_FR'] / 100)

def calcular_forma_repasse_k(row):
    return ((row['VL_PROCEDIMENTO'] * row['TX_PROCEDIMENTO_R'] / 100) #+
            # (row['VL_ANESTESISTA'] * row['TX_ANESTESISTA_R'] / 100) +
            # (row['VL_MATERIAIS'] * row['TX_MATERIAIS_R'] / 100) +
            # (row['VL_AUXILIARES'] * row['TX_AUXILIARES_R'] / 100) +
            # (row['VL_CUSTO_OPERACIONAL'] * row['TX_CUSTO_OPERACIONAL_R'] / 100)
            ) * (row['TX_REPASSE_FR'] / 100)

def calcular_forma_repasse_u(row):
    return (row['VL_REPASSE_R'] * (row['QT_PROCEDIMENTO'] * -1))

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

def calc_vl_repasse_all(connection, df_prod_regra, forma_repasse):
    try:

        # APENAS FORMAS DE REPASSE ATIVAS
        df_forma_repasse = forma_repasse[forma_repasse['IE_SITUACAO_FR'] == 'ATIVO'].copy()

        df_forma_repasse_terceiro = pd.merge(df_prod_regra, df_forma_repasse,
                                                left_on=["CD_REGRA_PREVISTO"],
                                                right_on=["CD_REGRA_FR"], how='left')
        
        # IDENTIFICAR ITENS A FATURAR QUE POSSUEM REGRA COM FORMA DE REPASSE ESPECIAL - NOTA: ESSA CHAMA UMA NOVA PROCEDURE NO BANCO
        terceiro_sregra_especial = df_forma_repasse_terceiro[df_forma_repasse_terceiro['IE_FORMA_CALCULO_R'] != 'R'].copy()
        terceiro_cregra_especial = df_forma_repasse_terceiro[df_forma_repasse_terceiro['IE_FORMA_CALCULO_R'] == 'R'].copy()

        # Inicializar DataFrames vazios
        forma_calculo_p = pd.DataFrame()
        forma_calculo_v = pd.DataFrame()
        forma_calculo_k = pd.DataFrame()
        forma_calculo_u = pd.DataFrame()
        forma_calculo_r = pd.DataFrame()
        forma_calculo_null = pd.DataFrame()

        # ITENS SEM REGRA ESPECIAL ENTÃO PRECISA REALIZAR CALCULO DA REGRA DE REPASSE
        if not terceiro_sregra_especial.empty:
            log_message(f"{len(terceiro_sregra_especial)} NAO TEM REGRA ESPECIAL DE REPASSE.")

            if (terceiro_sregra_especial['IE_FORMA_CALCULO_R'] == 'P').any():
                log_message("FORMA_CALCULO = P")
                forma_calculo_p = terceiro_sregra_especial[terceiro_sregra_especial['IE_FORMA_CALCULO_R'] == 'P'].copy()
                forma_calculo_p['TX_MEDICO_R'] = forma_calculo_p['TX_MEDICO_R'].fillna(0).astype(int)

                forma_calculo_p['VL_REPASSE_PREVISTO'] = forma_calculo_p.apply(calcular_forma_repasse_p, axis=1)

                log_message(len(forma_calculo_p))

            if (terceiro_sregra_especial['IE_FORMA_CALCULO_R'] == 'V').any():
                log_message("FORMA_CALCULO = V")
                forma_calculo_v = terceiro_sregra_especial[terceiro_sregra_especial['IE_FORMA_CALCULO_R'] == 'V'].copy()

                forma_calculo_v['VL_REPASSE_PREVISTO'] = forma_calculo_v.apply(calcular_forma_repasse_v, axis=1)

                log_message(len(forma_calculo_v))

            if (terceiro_sregra_especial['IE_FORMA_CALCULO_R'] == 'K').any():
                log_message("FORMA_CALCULO = K")
                forma_calculo_k = terceiro_sregra_especial[terceiro_sregra_especial['IE_FORMA_CALCULO_R'] == 'K'].copy()

                forma_calculo_k['VL_REPASSE_PREVISTO'] = forma_calculo_k.apply(calcular_forma_repasse_k, axis=1)

                log_message(len(forma_calculo_k))

            if (terceiro_sregra_especial['IE_FORMA_CALCULO_R'] == 'U').any():
                log_message("FORMA_CALCULO = U")
                forma_calculo_u = terceiro_sregra_especial[terceiro_sregra_especial['IE_FORMA_CALCULO_R'] == 'U'].copy()

                forma_calculo_u['VL_REPASSE_PREVISTO'] = forma_calculo_u.apply(calcular_forma_repasse_u, axis=1)

                log_message(len(forma_calculo_u))

            if terceiro_sregra_especial['IE_FORMA_CALCULO_R'].isnull().any():
                log_message("FORMA_CALCULO = NULL")
                forma_calculo_null = terceiro_sregra_especial[terceiro_sregra_especial['IE_FORMA_CALCULO_R'].isnull()].copy()

                forma_calculo_null.loc[:, 'VL_REPASSE_PREVISTO'] = 0

                log_message(len(forma_calculo_null))

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
  
                forma_calculo_r.loc[:, 'VL_REPASSE_PREVISTO'] = (forma_calculo_r['VL_REPASSE_ESPECIAL'] * forma_calculo_r['QT_PROCEDIMENTO']) * (forma_calculo_r['TX_REPASSE_FR'] / 100)
                log_message(len(forma_calculo_r))

        # DF A FATURAR FINAL
        list_dataframes = [forma_calculo_p, forma_calculo_v, forma_calculo_k, forma_calculo_u, forma_calculo_r, forma_calculo_null]
        # Fazer a união dos DataFrames não vazios
        df_vl_previstos = [df for df in list_dataframes if not df.empty]
        df_vl_previstos = pd.concat(df_vl_previstos, ignore_index=True)

        return df_vl_previstos

    except Exception as e:
        log_message(f"Erro ao calcular vl_repasse : {str(e)}")

def set_medico_forma_repasse(row):
    if pd.isnull(row['IE_BENEFICIARIO_FR']):
        return row['CD_MEDICO_EXECUTOR']
    elif row['IE_BENEFICIARIO_FR'] == 'E':
        return row['CD_MEDICO_EXECUTOR']
    else:
        return row['CD_PESSOA_FISICA_FR'] if not pd.isnull(row['CD_PESSOA_FISICA_FR']) else row['CD_MEDICO_EXECUTOR']

def create_file_zip(dir_csv,dir_file_zip, prefixo):
    # dir_csv = Especifique o diretório onde os arquivos CSV estão localizados
    # dir_file_zip = Especifique o nome do arquivo zip de saída
    # Cria um arquivo zip no modo de escrita
    with zipfile.ZipFile(dir_file_zip, 'w', zipfile.ZIP_DEFLATED) as arquivo_zip:
        # Percorre todos os arquivos no diretório especificado
        for pasta_raiz, pastas, arquivos in os.walk(dir_csv):
            for arquivo in arquivos:
                if arquivo.startswith(prefixo):  # Filtra arquivos pelo prefixo

                    caminho_arquivo = os.path.join(pasta_raiz, arquivo)
                    # Adiciona cada arquivo ao arquivo zip
                    arquivo_zip.write(caminho_arquivo, os.path.relpath(caminho_arquivo, dir_csv))
    log_message(f"Arquivo ZIP {dir_file_zip} criado com sucesso.")

def upload_zip_to_s3(dir_file_zip, dir_s3_zip):
    
    log_message("Enviando ZIP para S3")
    upload_to_s3(dir_file_zip, v_bucket_s3, dir_s3_zip)
    log_message(f"Arquivos csv enviados para AWS {dir_s3_zip}")

def chunk_data(data, chunk_size):
    """Divide os dados em lotes de até chunk_size registros."""
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

def prep_arquivos_camada_ingestion(loop, tbl_final, tbl_glosa):
    
    #dir_file_glosa = f'{dir_csv}\ing_glosa_{date_file}.csv'
    #save_to_csv(tbl_glosa, dir_file_glosa)
              
    # Cria o arquivo CSV para cada lote
    dir_file_prod_medica = f'{dir_csv}\{date_file}.csv'
    
    dir_s3_zip = f'raw/{dir_s3_date}/{date_file}.csv'
    save_to_csv(tbl_final, dir_file_prod_medica)
    upload_zip_to_s3(dir_file_prod_medica, dir_s3_zip)
    # if ambiente == "test":
    #     tbl_glosa.to_parquet(f'{dir_csv}\ing_glosa_{date_file}.parquet', compression='snappy', engine='pyarrow')
    #     tbl_final.to_parquet(f'{dir_csv}\ing_prod_medica_regras_formas_repasse_{date_file}.parquet', compression='snappy', engine='pyarrow')

    # Cria o arquivo zip
    #filename_zip = f'ing_files_{date_file}_{loop}.zip'
    #dir_file_zip = f'{dir_zip}\{filename_zip}' 
    #create_file_zip(dir_csv,dir_file_zip, 'ing_')
    
    # Envia o arquivo zip para o S3
    #dir_s3_zip = f'ingestion/{dir_s3_date}/{filename_zip}'
    #upload_zip_to_s3(dir_file_zip, dir_s3_zip)

    # CASO PRECISE ENVIAR EM PARTES, DESCOMENTAR ABAIXO
    # for idx, tbl in enumerate(chunk_data(tbl_final, v_chunck_size)):
    #     # Cria o arquivo CSV para cada lote
    #     dir_file_prod_medica = f'{dir_csv}\ing_prod_medica_regras_formas_repasse_{idx}_{date_file}.csv'
    #     save_to_csv(tbl, dir_file_prod_medica)
        
    #     # Cria o arquivo zip
    #     filename_zip = f'ing_files_{idx}_{date_file}.zip'
    #     dir_file_zip = f'{dir_zip}\{filename_zip}' 
    #     create_file_zip(dir_csv,dir_file_zip, 'ing_')
        
    #     # Envia o arquivo zip para o S3
    #     dir_s3_zip = f'ingestion/{dir_s3_date}/{filename_zip}'
    #     upload_zip_to_s3(dir_file_zip, dir_s3_zip)
             
def prep_arquivos_camada_raw(loop, tbl_regras_repasse, tbl_forma_repasse, tbl_prod_medica):
    log_message("Gerar CSV da camada raw")
    raw_file_regra_repasse = f'raw_regra_repasse_{date_file}.csv'
    dir_file_regra_repasse = f'{dir_csv}/{raw_file_regra_repasse}'
    save_to_csv(tbl_regras_repasse, dir_file_regra_repasse)

    raw_file_forma_repasse = f'raw_forma_repasse_{date_file}.csv'
    dir_file_forma_repasse = f'{dir_csv}/{raw_file_forma_repasse}'
    save_to_csv(tbl_forma_repasse, dir_file_forma_repasse)

    raw_file_prod_medica = f'raw_prod_medica_{date_file}.csv'
    dir_file_prod_medica = f'{dir_csv}/{raw_file_prod_medica}'
    save_to_csv(tbl_prod_medica, dir_file_prod_medica)
    
    # Cria o arquivo zip
    filename_zip = f'raw_files_{date_file}_{loop}.zip'
    dir_file_zip = f'{dir_zip}/{filename_zip}'
    create_file_zip(dir_csv,dir_file_zip, 'raw_')
    
    # Envia o arquivo zip para o S3
    dir_s3_zip = f'raw/{dir_s3_date}/{filename_zip}'
    upload_zip_to_s3(dir_file_zip, dir_s3_zip)

def log_message(msg, nivel='info'):
    print(msg)
    if nivel == 'info':
        logging.info(msg)
    elif nivel == 'error':
        logging.error(msg, exc_info=True)
        sys.exit(1)

# Função principal do ETL
def configure_exec():
    
    create_directory(v_dir_files_s3)

    # Obter a data e hora atual
    dt_inicio_exec_carga = datetime.now()
    global dt_execucao_carga_str
    dt_execucao_carga_str = dt_inicio_exec_carga.strftime('%Y-%m-%d %H:%M:%S')
    
    # Diretório do S3
    global dir_s3_date
    dir_s3_date = dt_inicio_exec_carga.strftime('%Y_%m_%d')
    
    global date_file
    date_file = dt_inicio_exec_carga.strftime('%Y_%m_%d_%H_%M_%S')
    # Configuração de logging
    filename_log=f'log_{date_file}.log'
    dir_file_log = f'{dir_logs}/{filename_log}' 
    logging.basicConfig(filename=dir_file_log, level=logging.INFO,
                        format='%(asctime)s - %(name)s -  %(levelname)s - %(message)s')
    
    # log_message(f"\nArquivo de Carga: {v_file_last_load}")
    # log_message(f"Diretório de saída: {v_dir_files_s3}")

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
            
            data_inicio_carga_glosa = '2025-11-01 00:00:00'

            log_message(f"Data de Última Carga  : {ult_carga}")
            log_message(f"Data de Última Carga c Atraso de 5 min: {ult_carga_c_atraso}")

            process_data(dt_inicio_exec_carga, data_inicio_carga, data_fim_carga, tipo_carga)
        else:
            data_inicio_carga = '2025-11-01 00:00:00'
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
        for medico in medicos[:10]:
            list_cpf_medicos.append(medico['cpf'])
        
        #loop = 0
        incr_loop = 0

        # Loop para percorrer o array de 50 em 50 posições
        for i in range(0, len(list_cpf_medicos), 50):
            # A variável 'i' será o índice de início de cada bloco de 50
            incr_loop += 1
            loop = str(incr_loop).zfill(2)
            print(f"\n\nIniciando processamento do bloco {loop} de médicos a partir da posição {i}")
            
            # Você também pode acessar um "bloco" de 50 elementos
            bloco = list_cpf_medicos[i:i+50]
            log_message(f"Bloco de médicos: {bloco}")
            log_message(f"Processando {len(bloco)} médicos a partir da posição {i}")
            log_message(f"Quantidade total de médicos : {len(bloco)}")
            tbl_dados_medicos = script0_obter_cd_medico(connection, bloco)
            df_cd_pessoa_fisica = tbl_dados_medicos[['CD_PESSOA_FISICA']]

            # 1: Obter regras de repasse para cada procedimento
            log_message('\n1: Gerar a tabela de produção médica')
            tbl_prod_medica = script1_obter_prod_medica(connection, df_cd_pessoa_fisica)
                            
            # 2: Gerar a tabela de regras e repasses com seus valores.
            log_message('\n2: Gerar a tabela de regras de repasses')
            tbl_regras_repasse = script2_obter_regras_repasse(connection)
        
            # 3: Obter regras de repasse para cada procedimento
            log_message('\n3: Gerar a tabela de formas de repasses')
            tbl_forma_repasse = script3_obter_forma_repasse(connection)
            
            # 4: Obter glosa
            log_message('\n4: Gerar a tabela de glosas')
            tbl_glosa =  script4_obter_glosa(connection)
    
            # 5: BASE FINAL
            tbl_final = gerar_base_final_c_valor_repasse(dt_execucao_carga_str, connection, tbl_prod_medica, tbl_regras_repasse, tbl_forma_repasse, tbl_dados_medicos)

            # 6: Gerar arquivos camada ingestion
            prep_arquivos_camada_ingestion(loop, tbl_final, tbl_glosa)

            # 7:  Gerar arquivos raw  - backup para caso de auditoria
            #prep_arquivos_camada_raw(loop, tbl_regras_repasse, tbl_forma_repasse, tbl_prod_medica)

            # 8: Exluir arquivos gerados
            excluir_arquivos_em_diretorio(dir_csv, pasta=True)
            excluir_arquivos_em_diretorio(dir_zip, pasta=True)
                    
        pool.release(connection)
        pool.close()
                
        # 9: Atualizar arquivo de última carga
        log_message(f"Atualizando arquivo de última carga: {v_file_last_load}")
        write_file_load(v_file_last_load, data_fim_carga)

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
    global v_dir_files_s3
    v_dir_files_s3 = os.getenv("DIR_FILES")
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