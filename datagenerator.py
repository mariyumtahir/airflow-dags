import os
import csv
import random
from datetime import datetime, timedelta

# Config
base_dir = "./data"
start_dt = datetime(2026, 5, 1, 0)
end_dt   = datetime(2026, 5, 6, 23)

names       = ["Alice","Bob","Charlie","Diana","Eve","Frank","Grace","Hank","Ivy","Jack"]
departments = ["Engineering","Marketing","Sales","HR","Finance","Operations"]

dt = start_dt
while dt <= end_dt:
    folder = os.path.join(base_dir, f"time_stamp={dt.strftime('%Y-%m-%d-%H')}")
    os.makedirs(folder, exist_ok=True)

    for file_idx in range(random.randint(1, 5)):
        filepath = os.path.join(folder, f"data_{file_idx + 1}.csv")
        with open(filepath, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name", "age", "department", "salary", "working_hours"])
            for row_id in range(1, random.randint(5, 20) + 1):
                writer.writerow([
                    row_id,
                    random.choice(names),
                    random.randint(22, 60),
                    random.choice(departments),
                    round(random.uniform(30000, 120000), 2),
                    round(random.uniform(20.0, 60.0), 1),
                ])

    dt += timedelta(hours=1)

print("Done!")
