import time
import datetime
import os
import pandas as pd
from catboost import CatBoostClassifier


def submit_predicts(submission):
    print(datetime.datetime.now(), 'Сохранить результаты предсказания')
    output_file_name = 'submissions/{}.csv'.format(int(time.time()))

    if not os.path.exists('submissions'):
        os.makedirs('submissions')

    print(output_file_name)
    submission.to_csv(output_file_name, index=True)
    

def pivot(transactions_totals, column):
    pivot = transactions_totals.reset_index().pivot(
        index='client_id',
        columns='small_group',
        values=column
    )
    pivot = pivot.fillna(0)
    pivot.columns = ['group_'+str(i)+'_'+column for i in pivot.columns]
    
    return pivot.reset_index()
    

def create_matrix(transactions_totals):
    print('Развернуть по transactions_count')
    matrix = pivot(transactions_totals, 'transactions_count')

    columns = ['mean_amount', 'std_amount', 'min_amount', 'max_amount']
    ##columns = ['mean_amount', 'std_amount']

    for column in columns:
        print('Развернуть по', column)
        matrix = pd.merge(
            matrix,
            pivot(transactions_totals, column),
            on='client_id'
        )
    
    return matrix.reset_index()


def create_train_set():
    print(datetime.datetime.now(), 'Сформировать данные для обучения модели')
    train_totals = pd.read_csv('./data/train_totals.csv')
    train_target = pd.read_csv('./data/train_target.csv')
    train = pd.merge(
        train_target,
        create_matrix(train_totals),
        on='client_id'
    )
    return train


def create_test_set():
    print(datetime.datetime.now(), 'Сформировать тестовый набор')
    test_totals = pd.read_csv('./data/test_totals.csv')
    test_id = pd.read_csv('./data/test.csv')
    test = pd.merge(
        test_id,
        create_matrix(test_totals),
        on='client_id'
    )
    return test


def fit_model(train, common_columns):
    print(datetime.datetime.now(), 'Обучение модели ...')
    print('Размер тренировочного набора:', train[common_columns].shape)
    model = CatBoostClassifier(verbose=True)
    model.fit(train[common_columns], train['bins'])
    return model


def predict(model, test):
    print(datetime.datetime.now(), 'Предказание ...')
    print('Размер тестового набора:', test[common_columns].shape)
    predicts = model.predict(test[common_columns])
    predicts = list(row[0] for row in predicts)
    return predicts


if __name__ == '__main__':
    train = create_train_set()
    test = create_test_set()
    common_columns = list(set(train.columns).intersection(set(test.columns)))

    model = fit_model(train, common_columns)
    predicts = predict(model, test)

    submission = pd.DataFrame({'bins': predicts}, index=test.client_id)
    submit_predicts(submission)
    
