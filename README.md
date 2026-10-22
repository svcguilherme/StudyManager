# StudyManager

Kanban de estudos **orientado a eventos**: cada ação (criar, começar, pausar, concluir) vira um
evento publicado no **Kafka**. O quadro é uma *projeção* desses eventos guardada no **PostgreSQL**.

```
 study (CLI) ──publica──▶ Kafka: tópico "study-events" (chave = item_id, 3 partições)
                               │
               ┌───────────────┴────────────────┐
               ▼                                ▼
     board-projector                    review-scheduler
     • grava no event store (events)    • ouve StudyCompleted
     • atualiza study_items (quadro)    • publica ReviewScheduled (+1, +3, +7, +15, +30 dias)
```

Conceitos praticados: producer/consumer, consumer groups, partições e chave de ordenação,
commit manual de offset (at-least-once), consumers idempotentes, Event Sourcing, CQRS,
projeções reconstruíveis e revisão espaçada.

## Subindo

```powershell
docker compose up -d                 # Redpanda (Kafka), Redpanda Console e Postgres
python -m venv .venv
.venv\Scripts\activate
pip install -e ".[dev]"
study init                           # cria tabelas e tópico
study worker                         # deixe rodando em outro terminal
```

Console do Kafka (tópicos, mensagens, offsets, consumer groups): http://localhost:8080
Postgres: `localhost:5433`, usuário/senha/banco `study` / `study` / `studymanager`.

## Uso

```powershell
study add "Kafka - Consumer Groups" -t kafka -t mensageria
study board
study start a1b2c3                    # aceita prefixo do id
study pause a1b2c3 -n "parei no rebalance"
study start a1b2c3
study done a1b2c3 -c 4 -n "falta praticar offsets"
study history a1b2c3                 # todos os eventos do item
study stats                          # tempo por tag e sequência de dias
study board --at 2026-10-01          # viagem no tempo: replay dos eventos até a data
study rebuild                        # apaga a projeção e reconstrói do event store
```

Colunas: **To Do → Doing → Done**, e o item volta sozinho para **To Review** quando a revisão vence.
Confiança 1–2 reinicia o ciclo de revisões.

Logo depois de um comando, o quadro pode levar um instante para refletir a mudança:
é a **consistência eventual** entre o Kafka e a projeção.

## Integração com o site (guilhermesantanna.dev.br)

```
 Admin do site ──▶ MySQL: study_items
                        ▲  GET /api/study/items?after=N   (site-importer, a cada SITE_POLL_SECONDS)
                        │
      site-importer ──publica StudyItemCreated──▶ Kafka ──▶ board-projector / review-scheduler
                                                     │
                     site-sync (consumer group) ◀────┘
                        │  POST /api/study/events   (evento + estado atual do item)
                        ▼
 MySQL: study_events + study_progress ──▶ aba "Progresso de estudos" no painel
```

- Configure `SITE_URL` e `SITE_API_KEY` no `.env` (modelo em `.env.example`). A chave é a
  `integrations.studymanager_key` do `config.php` do site, enviada no header `X-StudyManager-Key`.
- Com o site configurado, `study worker` também roda o `site-importer` e o `site-sync`.
- Itens do site entram com id `site-<id>` (ex.: `study start site-3`).
- **Site fora do ar:** o `site-sync` não confirma o offset e tenta de novo com espera crescente
  (até 60s). Os eventos ficam guardados no Kafka e são entregues quando o site voltar.
  O site descarta eventos repetidos pelo `event_id`, então reenviar é seguro.

## Quadro web e app instalado

`study app` sobe tudo de uma vez e abre o quadro em http://localhost:8092:

1. inicia o Docker Desktop, se estiver parado, e roda `docker compose up -d`;
2. cria as tabelas e o tópico, se faltarem;
3. sobe os workers (e a sincronização com o site, se configurada) e o quadro web (FastAPI).

Se o app já estiver rodando, só abre o navegador.

### Compilar e instalar

```powershell
.\build.ps1        # PyInstaller -> C:\myapps\studymanager\StudyManager.exe
```

A pasta instalada contém o `.exe`, o `docker-compose.yml`, o `.env` (preservado em reinstalações)
e `logs\studymanager.log`.

### Inicialização com o Windows

- Docker Desktop: opção "Start Docker Desktop when you sign in" ligada.
- StudyManager: atalho em `shell:startup` apontando para `C:\myapps\studymanager\StudyManager.exe`.
  Para desligar, apague o atalho dessa pasta.

Os containers têm `restart: unless-stopped`, então voltam junto com o Docker.

## Configuração (variáveis de ambiente)

| Variável | Padrão |
|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:19092` |
| `STUDY_TOPIC` | `study-events` |
| `DATABASE_URL` | `postgresql://study:study@localhost:5433/studymanager` |
| `WIP_LIMIT` | `2` |
| `REVIEW_UNIT` | `days` (use `minutes` para testar a revisão sem esperar) |
| `STUDY_TZ` | `America/Sao_Paulo` |
| `SITE_URL` | vazio (integração desligada) |
| `SITE_API_KEY` | vazio |
| `SITE_POLL_SECONDS` | `60` |
| `WEB_PORT` | `8092` (quadro web) |

## Testes

```powershell
pytest
```

## Próximos passos

- Outbox pattern no `review-scheduler` (gravar o evento no banco e publicar depois)
- Dependências entre itens (trilhas de estudo)
- Dashboard no Grafana / API com FastAPI
- Schema Registry com Avro para versionar os eventos
