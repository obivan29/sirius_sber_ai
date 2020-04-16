--	 train_totals
Copy (
	SELECT trn.client_id, trn.small_group
			, COUNT(*) AS transactions_count
			, SUM(trn.amount_rur) AS total_amount
			, SUM(trn.amount_rur) / COUNT(*) AS mean_amount
			, coalesce(stddev_samp(trn.amount_rur), 0) AS std_amount
			, MIN(trn.amount_rur) AS min_amount
			, MAX(trn.amount_rur) AS max_amount
		FROM sber.transactions_train AS trn
		GROUP BY trn.client_id, trn.small_group
		ORDER BY trn.client_id, trn.small_group
) To 'С:/age/data/train_totals.csv' With CSV HEADER DELIMITER ',';

-- train_totals_by_period
Copy (
	SELECT concat(trn.client_id, ':', trans_date/73) as client_index, trans_date/73 as period, trn.small_group
			, COUNT(*) AS transactions_count
			, SUM(trn.amount_rur) AS total_amount
			, SUM(trn.amount_rur) / COUNT(*) AS mean_amount
			, coalesce(stddev_samp(trn.amount_rur), 0) AS std_amount
			, MIN(trn.amount_rur) AS min_amount
			, MAX(trn.amount_rur) AS max_amount
		FROM sber.transactions_train AS trn
		GROUP BY 1, 2, 3
		ORDER BY 1, 2, 3
) To 'C:/age/data/train_totals_by_period.csv' With CSV HEADER DELIMITER ',';

-- train_target_by_period
Copy (
	SELECT concat(tar.client_id, ':', periods.period) as client_index, periods.period as period, tar.bins 
		FROM sber.train_target AS tar
			JOIN (SELECT DISTINCT trans_date/73 AS period FROM sber.transactions_train) AS periods ON 1=1
		ORDER BY 1, 2
) To 'c:/age/data/train_target_by_period.csv' With CSV HEADER DELIMITER ',';

-- test_totals
Copy (
	SELECT trn.client_id, trn.small_group
			, COUNT(*) AS transactions_count
			, SUM(trn.amount_rur) AS total_amount
			, SUM(trn.amount_rur) / COUNT(*) AS mean_amount
			, coalesce(stddev_samp(trn.amount_rur), 0) AS std_amount
			, MIN(trn.amount_rur) AS min_amount
			, MAX(trn.amount_rur) AS max_amount
		FROM sber.transactions_test AS trn
		GROUP BY trn.client_id, trn.small_group
		ORDER BY trn.client_id, trn.small_group
) To 'c:/age/data/test_totals.csv' With CSV HEADER DELIMITER ',';

-- test_totals_by_period
COPY (
	SELECT concat(trn.client_id, ':', trans_date/73) as client_index, trans_date/73 as period, trn.small_group
			, COUNT(*) AS transactions_count
			, SUM(trn.amount_rur) AS total_amount
			, SUM(trn.amount_rur) / COUNT(*) AS mean_amount
			, coalesce(stddev_samp(trn.amount_rur), 0) AS std_amount
			, MIN(trn.amount_rur) AS min_amount
			, MAX(trn.amount_rur) AS max_amount
		FROM sber.transactions_test AS trn
		GROUP BY 1, 2, 3
		ORDER BY 1, 2, 3
) To 'c:/age/data/test_totals_by_period.csv' With CSV HEADER DELIMITER ',';

-- test_index
COPY (
	SELECT distinct concat(trn.client_id, ':', trans_date/73) AS client_index, trans_date/73 AS period
		FROM sber.transactions_test AS trn
		ORDER BY 1
) TO 'c:/age/data/test_index.csv' WITH CSV HEADER DELIMITER ',';
