"""
Módulo utilitário para executar comandos DBT e gerenciar projetos DBT.
"""

import os
import subprocess
import json
from src.utils.logger import setup_logger

# Configurar logger
logger = setup_logger(__name__)

class DBTRunner:
    """
    Classe para executar comandos DBT e gerenciar projetos DBT.
    """

    def __init__(self, project_dir=None, profiles_dir=None):
        """
        Inicializa o DBTRunner.

        Args:
            project_dir (str, optional): Caminho para o diretório do projeto DBT
            profiles_dir (str, optional): Caminho para o diretório de perfis DBT
        """
        self.project_dir = project_dir or os.path.join(os.getcwd(), "models")
        self.profiles_dir = profiles_dir or os.path.join(os.getcwd(), "models", "profiles")

        # Garantir que os diretórios existam
        os.makedirs(self.project_dir, exist_ok=True)
        os.makedirs(self.profiles_dir, exist_ok=True)

        logger.info(f"DBTRunner inicializado com project_dir={self.project_dir}, profiles_dir={self.profiles_dir}")

    def _run_command(self, command, cwd=None):
        """
        Executa um comando de shell e retorna a saída.

        Args:
            command (list): Comando a ser executado como uma lista de strings
            cwd (str, optional): Diretório de trabalho para executar o comando

        Returns:
            tuple: (return_code, stdout, stderr)
        """
        logger.info(f"Executando comando: {' '.join(command)}")

        try:
            # Usar apenas o DBT Core local
            env = os.environ.copy()

            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=cwd or self.project_dir,
                env=env
            )

            # Obter a saída
            stdout, stderr = process.communicate()
            return_code = process.returncode

            # Registrar o resultado
            if return_code == 0:
                logger.info(f"Comando concluído com sucesso com código de retorno {return_code}")
            else:
                logger.error(f"Comando falhou com código de retorno {return_code}")
                logger.error(f"stdout: {stdout}")
                logger.error(f"stderr: {stderr}")

            return return_code, stdout, stderr

        except Exception as e:
            logger.error(f"Erro ao executar comando: {str(e)}")
            raise

    def init_project(self, project_name):
        """
        Inicializa um novo projeto DBT.

        Args:
            project_name (str): Nome do projeto DBT

        Returns:
            bool: True se bem-sucedido, False caso contrário
        """
        logger.info(f"Inicializando projeto DBT: {project_name}")

        try:
            # Executar comando dbt init
            return_code, stdout, stderr = self._run_command(
                ["dbt", "init", project_name],
                cwd=os.path.dirname(self.project_dir)
            )

            # Verificar se foi bem-sucedido
            if return_code == 0:
                logger.info(f"Projeto DBT inicializado com sucesso: {project_name}")

                # Atualizar diretório do projeto
                self.project_dir = os.path.join(os.path.dirname(self.project_dir), project_name)

                return True
            else:
                logger.error(f"Falha ao inicializar projeto DBT: {stderr}")
                return False

        except Exception as e:
            logger.error(f"Erro ao inicializar projeto DBT: {str(e)}")
            raise

    def create_profile(self, profile_name, target_name, type="spark", threads=4, schema="public", **kwargs):
        """
        Cria um perfil DBT para conexão com um banco de dados.

        Args:
            profile_name (str): Nome do perfil
            target_name (str): Nome do alvo
            type (str): Tipo de banco de dados (spark, postgres, etc.)
            threads (int): Número de threads a serem usadas
            schema (str): Schema padrão a ser usado
            **kwargs: Parâmetros adicionais de conexão

        Returns:
            bool: True se bem-sucedido, False caso contrário
        """
        logger.info(f"Criando perfil DBT: {profile_name}")

        try:
            # Criar diretório de perfil se não existir
            os.makedirs(self.profiles_dir, exist_ok=True)

            # Criar caminho do arquivo de perfil
            profile_path = os.path.join(self.profiles_dir, f"{profile_name}.yml")

            # Criar conteúdo do perfil
            profile = {
                profile_name: {
                    "target": target_name,
                    "outputs": {
                        target_name: {
                            "type": type,
                            "threads": threads,
                            "schema": schema,
                            **kwargs
                        }
                    }
                }
            }

            # Escrever perfil no arquivo
            with open(profile_path, "w") as f:
                import yaml
                yaml.dump(profile, f, default_flow_style=False)

            logger.info(f"Perfil DBT criado com sucesso: {profile_path}")
            return True

        except Exception as e:
            logger.error(f"Erro ao criar perfil DBT: {str(e)}")
            raise

    def run_models(self, models=None, exclude=None, vars=None, full_refresh=False):
        """
        Executa modelos DBT.

        Args:
            models (list, optional): Lista de modelos para executar
            exclude (list, optional): Lista de modelos para excluir
            vars (dict, optional): Variáveis para passar ao DBT
            full_refresh (bool): Se deve fazer uma atualização completa

        Returns:
            bool: True se bem-sucedido, False caso contrário
        """
        logger.info("Executando modelos DBT")

        try:
            # Construir comando
            command = ["dbt", "run", "--profiles-dir", self.profiles_dir]

            # Adicionar modelos se especificados
            if models:
                command.extend(["--models", " ".join(models)])

            # Adicionar exclusões se especificadas
            if exclude:
                command.extend(["--exclude", " ".join(exclude)])

            # Adicionar variáveis se especificadas
            if vars:
                command.extend(["--vars", json.dumps(vars)])

            # Adicionar full-refresh se especificado
            if full_refresh:
                command.append("--full-refresh")

            # Executar comando
            return_code, stdout, stderr = self._run_command(command)

            # Verificar se foi bem-sucedido
            if return_code == 0:
                logger.info("Modelos DBT executados com sucesso")
                return True
            else:
                logger.error(f"Falha ao executar modelos DBT: {stderr}")
                return False

        except Exception as e:
            logger.error(f"Erro ao executar modelos DBT: {str(e)}")
            raise

    def test_models(self, models=None, exclude=None):
        """
        Testa modelos DBT.

        Args:
            models (list, optional): Lista de modelos para testar
            exclude (list, optional): Lista de modelos para excluir

        Returns:
            bool: True se bem-sucedido, False caso contrário
        """
        logger.info("Testando modelos DBT")

        try:
            # Construir comando
            command = ["dbt", "test"]

            # Adicionar modelos se especificados
            if models:
                command.extend(["--models", " ".join(models)])

            # Adicionar exclusões se especificadas
            if exclude:
                command.extend(["--exclude", " ".join(exclude)])

            # Executar comando
            return_code, stdout, stderr = self._run_command(command)

            # Verificar se foi bem-sucedido
            if return_code == 0:
                logger.info("Modelos DBT testados com sucesso")
                return True
            else:
                logger.error(f"Falha ao testar modelos DBT: {stderr}")
                return False

        except Exception as e:
            logger.error(f"Erro ao testar modelos DBT: {str(e)}")
            raise
