#!/usr/bin/env bash

set -Eeuo pipefail

: "${AIRFLOW_ADMIN_USER:?AIRFLOW_ADMIN_USER must be set}"
: "${AIRFLOW_ADMIN_PASSWORD:?AIRFLOW_ADMIN_PASSWORD must be set}"
: "${AIRFLOW_ADMIN_EMAIL:?AIRFLOW_ADMIN_EMAIL must be set}"

# Экспортируем список существующих пользователей Airflow.
users_file="$(mktemp)"
trap 'rm -f "$users_file"' EXIT

airflow users export "$users_file"

# Проверяем существование пользователя и наличие роли Admin.
user_state="$(
python - "$AIRFLOW_ADMIN_USER" "$users_file" <<'PY'
import json
import sys

username = sys.argv[1]
filepath = sys.argv[2]

with open(filepath, encoding="utf-8") as file:
    users = json.load(file)

for user in users:
    if user.get("username") == username:
        if "Admin" in user.get("roles", []):
            print("admin")
        else:
            print("exists")
        break
else:
    print("missing")
PY
)"

# Создаём администратора или восстанавливаем его роль.
case "$user_state" in
    missing)
        echo "Creating Airflow admin: $AIRFLOW_ADMIN_USER"

        airflow users create \
            --username "$AIRFLOW_ADMIN_USER" \
            --firstname "Airflow" \
            --lastname "Admin" \
            --role Admin \
            --email "$AIRFLOW_ADMIN_EMAIL" \
            --password "$AIRFLOW_ADMIN_PASSWORD"
        ;;

    exists)
        echo "Adding Admin role to: $AIRFLOW_ADMIN_USER"

        airflow users add-role \
            --username "$AIRFLOW_ADMIN_USER" \
            --role Admin
        ;;

    admin)
        echo "Airflow admin already exists: $AIRFLOW_ADMIN_USER"
        ;;

    *)
        echo "ERROR: Unexpected user state: $user_state" >&2
        exit 1
        ;;
esac

echo "Airflow user initialization completed."