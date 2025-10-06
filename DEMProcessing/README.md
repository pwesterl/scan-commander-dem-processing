          ┌─────────────┐
          │ File System │
          │ AW_bearbetn│
          │    ing_test│
          └─────┬──────┘
                │
                ▼
          ┌─────────────┐
          │  Watcher    │
          │ (watcher.py)│ 1 container
          └─────┬──────┘
                │ inserts
                ▼
          ┌─────────────┐
          │  PostgreSQL │
          │   (db)      │  TODO : fixa prioritet
          └─────┬──────┘
                │
                ▼
          ┌─────────────┐
          │ Publisher   │ 1 container 
          │ publisher.py│
          └─────┬──────┘
                │ publishes jobs
                ▼
          ┌─────────────┐
          │ RabbitMQ    │
          │ (broker)    │ 1 service 
          └─────┬──────┘
          ┌─────┴───────┐
          │             │
          ▼             ▼
 ┌────────────────┐ ┌────────────────┐
 │ Preprocess     │ │ Inference      │
 │ Worker         │ │ Worker         │
 │ (process_worker│ │ (inference_wor │
 │ .py)           │ │ ker.py)        │
 └───────────────┘  └───────────────┘
          │                  │
          ▼                  ▼
   ┌─────────────┐     ┌─────────────┐
   │ Preprocessed│     │ Inference   │
   │ Images      │     │ Output      │
   └─────────────┘     └─────────────┘


todo:

* fixa sweref scriptet så vi kan köra mot skog-nas
* fixa så vi har både preprocess 10 och 20cm 
* fixa så vi kör inferens på separata modeller (inference_out_kolbotten + (inference_out_fangst)
* kolla watcher -> om fil kopieras in på skog-nas01, hur fungerar williams-script och preprocess när filen är "in-copy"
* prioritet i DB - \\skog-nas01\Data\scan-data\leverans-2025\downloaded.txt