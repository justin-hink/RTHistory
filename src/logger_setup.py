# logger_setup.py
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime
import logging
from pathlib import Path
import sys
if getattr(sys, 'frozen', False):
    # Running inside PyInstaller
    PARENT_DIRECTORY = Path(sys.executable).parent
else:
    # Running as normal script
    PARENT_DIRECTORY = Path(__file__).parent
LOGS_DIRECTORY = PARENT_DIRECTORY / "logs" 
LOGS_DIRECTORY.mkdir(exist_ok=True)
LOGS_PATH = LOGS_DIRECTORY / "logs.db"
Base = declarative_base()
engine = create_engine(f"sqlite:///{LOGS_PATH}", echo=False)
Session = sessionmaker(bind=engine)

class LogRecord(Base):
    __tablename__ = "logs"
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    logger_name = Column(String(50))
    level = Column(String(10))
    message = Column(Text)
    extra = Column(Text)

Base.metadata.create_all(engine)

class SQLAlchemyHandler(logging.Handler):
    """Custom logging handler that writes logs to SQLAlchemy DB."""
    def emit(self, record):
        session = Session()
        try:
            extra = getattr(record, "extra", None)
            log = LogRecord(
                timestamp=datetime.utcfromtimestamp(record.created),
                logger_name=record.name,
                level=record.levelname,
                message=self.format(record),
                extra=str(extra) if extra else None,
            )
            session.add(log)
            session.commit()
        finally:
            session.close()

def get_sqlalchemy_logger(name: str, level=logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not any(isinstance(h, SQLAlchemyHandler) for h in logger.handlers):
        handler = SQLAlchemyHandler()
        formatter = logging.Formatter("%(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False
    return logger

core_logger = get_sqlalchemy_logger("Core", level=logging.DEBUG)
# Predefine common loggers
TaskManager_task_logger = get_sqlalchemy_logger("TaskManager", level=logging.DEBUG)
SCP_task_logger = get_sqlalchemy_logger("StoreSCP", level=logging.DEBUG)
SCU_task_logger = get_sqlalchemy_logger("QueryRetrieveSCU", level=logging.DEBUG)
pdf_logger = get_sqlalchemy_logger("PDF_Generator", level=logging.DEBUG)
