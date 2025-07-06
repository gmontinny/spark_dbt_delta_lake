"""
Módulo utilitário para configurar logs consistentes em toda a aplicação.
"""

import os
import logging
from config.config import APP_CONFIG

def setup_logger(name, log_file=None):
    """
    Configura um logger com formatação e manipuladores consistentes.

    Args:
        name (str): Nome do logger, tipicamente __name__
        log_file (str, optional): Nome do arquivo de log. Se None, usa o parâmetro name.

    Returns:
        logging.Logger: Logger configurado
    """
    # Criar logger
    logger = logging.getLogger(name)

    # Configurar apenas se ainda não estiver configurado
    if not logger.handlers:
        # Definir nível a partir da configuração
        logger.setLevel(getattr(logging, APP_CONFIG['log_level']))

        # Criar formatador
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        # Criar manipulador de arquivo
        if log_file is None:
            log_file = f"{name.split('.')[-1]}.log"

        file_handler = logging.FileHandler(os.path.join('logs', log_file))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Criar manipulador de console
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger
