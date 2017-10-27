# -*- coding: utf-8 -*-
"""
Created on Thu Oct 26 13:47:34 2017

@author: ligong

@description:这是运行FTRL predict的程序
"""
import sys
import json
import redis
from ftrl_sigmoid import ftrl_sigmoid
from ftrl_predict import ftrl_predict

def load_config(config_path):
    """
    加载配置文件:
        {
                "w_redis_url":"redis://127.0.0.1:/0","ftrl_gd_threads":10,
                "z_redis_url":"redis://127.0.0.1:/1","ftrl_weight_updater_threads":10,
                "n_redis_url":"redis://127.0.0.1:/2","ftrl_train_threads":10,
                "train_redis_url":"redis://127.0.0.1:/3","ftrl_predict_threads":10,
                "predict_redis_url":"redis://127.0.0.1:/4","ftrl_sigmoid_threads":10
        }
    """
    return json.load(open(config_path))


def init_redis(url):
    """
    获得redis链接
    """
    return redis.Redis.from_url(url)

def run_threads(threads):
    """
    运行thread
    """
    for t in threads:
        t.start()

def join_threads(threads):
    """
    join thread
    """
    for t in threads:
        t.join()
        
def process(config):
    """
    运行
    """
    w_redis_conn = init_redis(config['w_redis_url'])
    z_redis_conn = init_redis(config['z_redis_url'])
    n_redis_conn = init_redis(config['n_redis_url'])
    predict_redis_conn = init_redis(config['predict_redis_url'])
    
    
    ftrl_predict_threads,ftrl_sigmoid_threads = [],[]
    
    #生成ftrl_sigmoid_threads
    for i in range(config['ftrl_sigmoid_threads']):
        ftrl_sigmoid_threads.append(ftrl_sigmoid('ftrl_sigmoid_thread_%s' % i,w_redis_conn))
        
    #生成ftrl_predict_threads
    for i in range(config['ftrl_predict_threads']):
        ftrl_predict_threads.append(ftrl_predict('ftrl_predict_thread_%s' % i,w_redis_conn,ftrl_sigmoid_threads,predict_redis_conn))
        
    run_threads(ftrl_predict_threads)
    run_threads(ftrl_sigmoid_threads)
    join_threads(ftrl_sigmoid_threads)
    join_threads(ftrl_predict_threads)
    
if __name__ == '__main__':
    config_path = sys.argv[1]
    config = load_config(config_path)
    process(config)
    
