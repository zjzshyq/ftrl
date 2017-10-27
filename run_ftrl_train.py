# -*- coding: utf-8 -*-
"""
Created on Thu Oct 26 13:47:34 2017

@author: ligong

@description:这是运行FTRL Train的程序
"""
import sys
import json
import redis
from ftrl_gd import ftrl_gd
from ftrl_weight_updater import ftrl_weight_updater
from ftrl_train import ftrl_train

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
    train_redis_conn = init_redis(config['train_redis_url'])
    
    
    ftrl_gd_threads,ftrl_weight_updater_threads,ftrl_train_threads = [],[],[]
    #生成ftrl_gd_threads
    for i in range(config['ftrl_gd_threads']):
        ftrl_gd_threads.append(ftrl_gd('ftrl_gd_thread_%s' % i,w_redis_conn,z_redis_conn,n_redis_conn))
        
    #生成ftrl_weight_updater_threads
    for i in range(config['ftrl_weight_updater_threads']):
        ftrl_weight_updater_threads.append(ftrl_weight_updater('ftrl_updater_thread_%s' % i,w_redis_conn,z_redis_conn,n_redis_conn))
        
    #生成ftrl_train_threads
    for i in range(config['ftrl_train_threads']):
        ftrl_train_threads.append(ftrl_train('ftrl_train_thread_%s' % i,w_redis_conn,z_redis_conn,n_redis_conn,train_redis_conn,ftrl_weight_updater_threads,ftrl_gd_threads))
        
    run_threads(ftrl_gd_threads)
    run_threads(ftrl_weight_updater_threads)
    run_threads(ftrl_train_threads)
    
    join_threads(ftrl_gd_threads)
    join_threads(ftrl_weight_updater_threads)
    join_threads(ftrl_train_threads)
    
if __name__ == '__main__':
    config_path = sys.argv[1]
    config = load_config(config_path)
    process(config)
    
