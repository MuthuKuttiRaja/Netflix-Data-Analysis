from pyspark.sql import SparkSession
from collections import defaultdict
import sys
import pandas as pd

m_movies_b = None
def movie_ref_mapper_func(value):
    return (value[0], {'movie_id': value[0],
                   'publication': value[1],
                   'movie_name': value[2]})

def parse_csv_mapper_func(value):
    movie_id = ''
    out = []
    for index, line in enumerate(value.value.split('\n')):
        if index == 0:
            movie_id = line.strip(':')
        else:
            value_splits = line.split(',')
            if len(value_splits)==3:
                out.append({'movie_id': movie_id,
                            'user_id': value_splits[0],
                            'rating': value_splits[1],
                            'date': value_splits[2]} )
    return out

def movies_mapper_func(value):
    return (value['movie_id'], value)

def user_mapper_func(value):
    return (value['user_id'], value)

def movies_reducer_func(value):
    cnt = 0
    rating_total = 0
    movie_name = ''
    movie_publication = 0

    for val in value[1][1]:
        movie_name = val['movie_name']
        movie_publication = int(val['publication']) if val['publication'] != 'NULL' else 0

    for val in value[1][0]:
        cnt += 1
        rating_total += int(val['rating'])

    avg = rating_total / cnt
    # return (v[0], movie_name, movie_publication, cnt, avg)
    return ({'movie_id': value[0],
             'movie_name': movie_name,
             'movie_publication': movie_publication,
             'cnt': cnt,
             'avg': avg})

def user_reducer_func(value):
    global m_movies_b
    m_movies = m_movies_b.value

    movies_lst = []
    ratings_dict = defaultdict(list)
    rating_sum = 0
    intersect_count = 0

    for val in value[1]:
        rating = int(val['rating'])
        ratings_dict[rating].append({'movie_id': val['movie_id'],
                                     'rating-dt': val['date']})
        if val['movie_id'] in m_movies:
            intersect_count += 1
            rating_sum += rating

    max_rating = max(ratings_dict.keys())
    highest_rated_films = ratings_dict[max_rating]

    return ({'user_id': value[0],
             'highest_rated_film': highest_rated_films,
             'intersect_count': intersect_count,
             'm_movies_avg_rating': float(rating_sum) / intersect_count if intersect_count > 0 else 0,
             'highest_rating': max_rating,
             'total': len(value[1])})

def get_top_m_movies(M, R, lst):
    lst = sorted(lst, key=lambda x: (-x['cnt'], -int(x['movie_publication']), x['movie_name']))
    return set([x['movie_id'] for x in lst if x['cnt'] >= R][:M])

def get_top_u_users(M, U, lst, movies_dict):
    lst = [v for v in lst if v['intersect_count'] == M]
    lst = sorted(lst, key=lambda x: (float(x['m_movies_avg_rating']), int(x['user_id'])))

    out = []
    for i, v in enumerate(lst):
        if i == U:
            break

        movie_lst = []
        for movie in v['highest_rated_film']:
            movie_lst.append({'rating_dt': movie['rating-dt'],
                              'publication': movies_dict[movie['movie_id']]['publication'],
                              'movie_name': movies_dict[movie['movie_id']]['movie_name']
                              })
        movie = sorted(movie_lst, key=lambda x: (-int(x['publication']), x['movie_name']))[0]
        out.append({'user_id': v['user_id'],
                    'year_release': movie['publication'],
                    'rating_dt': movie['rating_dt'],
                    'movie': movie['movie_name'],
                    'm_movies_avg_rating': v['m_movies_avg_rating'],
                    'highest_rating': v['highest_rating']})
    return out

def get_movies_dict(movies_ref_rdd):
    movies_df = movies_ref_rdd.toDF().toPandas()
    movies_dict = {}
    for indx, row in movies_df.iterrows():
        movies_dict[row[0]] = {'publication': row[1],
                               'movie_name': row[2]}
    return movies_dict

def main():
    args = sys.argv[1:]
    spark = SparkSession.builder.appName("netflix_data_analysis").getOrCreate()
    movies_rdd = spark.read.text(args[3], wholetext=True).rdd
    movies_ref_rdd = spark.read.csv(args[4]).rdd
    movie_ref_mapper_rdd = movies_ref_rdd.map(movie_ref_mapper_func).groupByKey()
    movies_dict = get_movies_dict(movies_ref_rdd)
    parse_csv_mapper_rdd = movies_rdd.flatMap(parse_csv_mapper_func).cache()
    movies_mapper_rdd = parse_csv_mapper_rdd.map(movies_mapper_func).groupByKey()
    movies_reducer_rdd = movies_mapper_rdd.join(movie_ref_mapper_rdd).map(movies_reducer_func)
    movies_list = spark.createDataFrame(movies_reducer_rdd).toPandas().to_dict('records')
    M_conf = int(args[0])
    R_conf = int(args[1])
    U_conf = int(args[2])

    global m_movies_b
    m_movies = get_top_m_movies(M_conf, R_conf, movies_list)
    m_movies_b = spark.sparkContext.broadcast(m_movies)
    user_mapper_rdd = parse_csv_mapper_rdd.map(user_mapper_func).groupByKey()
    user_reducer_rdd = user_mapper_rdd.map(user_reducer_func)
    users_list = spark.createDataFrame(user_reducer_rdd).toPandas().to_dict('records')
    top_u_users_lst = get_top_u_users(M_conf, U_conf, users_list, movies_dict)
    df = pd.DataFrame(top_u_users_lst)
    df.to_csv(args[5], index=False, sep= '\t', columns=['user_id', 'movie', 'year_release', 'rating_dt'] )


if __name__ == '__main__':
    main()


