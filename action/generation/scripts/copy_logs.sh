_clear_log_folder() {
  sudo bash -c "rm -rf /var/lib/postgresql/14/main/log/*"
  echo "Cleared all query logs."
}

_copy_logs() {
  save_path="${1}"

  # TODO(WAN): Is there a way to ensure all flushed?
  sleep 10
  sudo bash -c "cat /var/lib/postgresql/14/main/log/*.csv > ${save_path}"
  echo "Copied all query logs to: ${save_path}"
}

_clear_log_folder
doit project1_enable_logging
doit benchbase_run --benchmark="ycsb" --config="./action/generation/ycsb_config.xml" --args="--execute=true"
doit project1_disable_logging
_copy_logs "./artifacts/action/ycsb_trace.csv"