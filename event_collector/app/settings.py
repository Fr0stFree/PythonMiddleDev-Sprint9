from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    debug: bool = False
    app_port: int = 8080
    app_host: str = 'localhost'
