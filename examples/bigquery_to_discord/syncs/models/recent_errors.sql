-- Replace with your actual BigQuery project, dataset, and table
SELECT
    id,
    error_message,
    service_name,
    created_at
FROM `your_project.your_dataset.app_logs`
WHERE status = 'error'
