# -*- coding: utf-8 -*-

import os
import asyncio
from pathlib import Path
from dotenv import load_dotenv

from browser_use import Agent, Browser, ChatBrowserUse


async def main():
    # força carregar o .env da raiz do projeto (Bots), mesmo se o PyCharm mudar o "Working directory"
    root_dir = Path(__file__).resolve().parents[1]      # ...\Bots
    env_path = root_dir / ".env"
    load_dotenv(dotenv_path=env_path, override=True)

    api_key = os.getenv("BROWSER_USE_API_KEY", "bu_NiJzfGjQdt3GhGm3XJ4p3gsOgO8wVUzi_P3tEDZdfZA").strip()
    if not api_key:
        raise RuntimeError(
            "BROWSER_USE_API_KEY não foi carregada.\n"
            f"Confirme se existe: {env_path}\n"
            "E se o arquivo chama exatamente .env (sem .txt)."
        )

    browser = Browser(
        headless=False,
        window_size={"width": 1200, "height": 850},
    )

    llm = ChatBrowserUse()

    task = """
    Vá para https://basedosdados.org/search
    Se aparecer banner de cookies, aceite/feche.

    Na busca, pesquise exatamente por: Avaliação da Alfabetização
    Abra o resultado correspondente ao dataset (o que tem esse nome).

    Na página do dataset, extraia:
    - url
    - titulo
    - organizacao
    - cobertura_temporal
    - resumo (primeiro parágrafo/descrição)

    Retorne APENAS um JSON com essas chaves.
    """

    agent = Agent(
        task=task,
        llm=llm,
        browser=browser,
        use_vision=True,     # ajuda bastante em páginas dinâmicas
        step_timeout=180,    # dá mais tempo pra carregar
        max_failures=5,
    )

    result = await agent.run(max_steps=60)
    print(result)


if __name__ == "__main__":
    asyncio.run(main())