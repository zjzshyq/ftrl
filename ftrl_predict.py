# -*- coding: utf-8 -*-
"""
Created on Thu Oct 26 11:40:48 2017

@author: ligong

@description:这是FTRL在线预测和测试的程序
"""

import json
import time
import threading
import traceback
import random
from sklearn import metrics

class ftrl_predict(threading.Thread):
    def __init__(self,thread_name,w_redis_conn,sigmoid_threads,predictjob_redis_conn,max_test = 100000):
        """
        初始化线程
        thread_name:线程名
        w_redis_conn:w参数的存放redis
        predictjob_redis_conn:任务的队列
        sigmoid_threads:计算的线程
        """
        super(ftrl_predict, self).__init__(name = thread_name)
        self.w_redis_conn = w_redis_conn
        self.predictjob_redis_conn = predictjob_redis_conn
        self.sigmoid_threads = sigmoid_threads
        self.setDaemon = True
        
        #最大测试量
        self.max_test = max_test
        
        #任务队列
        self.job_queue_name = 'PREDICT_JOB_QUEUE'
        
        
        #auc计算的缓存
        self.auc_cache = 'AUC_CACHE'
        
        #是否停止更新
        self.stop_update = False
        
        #杀死
        self.dead = False
        
        #每次睡眠时间
        self.sleep_time = 10
        
        self.is_test = True
 
    def get_thread(self,threads):
        """
        随机获得一个线程
        """
        n = len(threads)
        idx = random.randint(0,n-1)
        return threads[idx]
    
    def stop(self):
        """
        停止更新
        """
        self.stop_update = True
    
    def restart(self):
        """
        继续更新
        """
        self.stop_update = False
    
    def set_test(self,is_test):
        """
        设置是否是测试
        """
        self.is_test = is_test
        
        
    def kill(self):
        """
        杀死
        """
        self.dead = True
    
    def calcu_auc(self):
        """
        计算auc
        """
        y,y_test = [],[]
        
        for item in self.predictjob_redis_conn.lrange(self.auc_cache,0,-1):
            try:
                t = eval(item)
            except:
                continue
            y.append(t[0])
            y_test.append(t[1])
        tmp = map(lambda _:1 if _ > 0.5 else 0 ,list(y_test))
        precision = metrics.precision_score(y, tmp)  
        recall = metrics.recall_score(y, tmp) 
        print '\n\n\n'
        print 'precision:',precision
        print 'recall:',recall
        print 'f1:', 2.0/(1.0/precision+1.0/recall)
        print 'auc:',metrics.roc_auc_score(y,y_test) 
        print '\n\n\n'
        #return metrics.roc_auc_score(y_test, y)
    
    def add_to_result_set(self,y,y_test):
        """
        添加到结果集合
        """
        self.predictjob_redis_conn.rpush(self.auc_cache,(y,y_test))
        if self.predictjob_redis_conn.llen(self.auc_cache) > self.max_test:
            self.predictjob_redis_conn.lpop(self.auc_cache)
    
    def get_predict_job(self,job_str):
        try:
            job = json.loads(job_str)
            data_dict = job['data']
            y = job['label']
            job_thread = self.get_thread(self.sigmoid_threads)
            if self.is_test:
                job = [data_dict,self.add_to_result_set,y]
            else:
                job = [data_dict,None,y]
            job_thread.add_job(job)
            return True
        except:
            traceback.print_exc()
            return False

    def run(self):
        print '%s is running ...' % self.getName()
        iter_number = 1
        while not self.dead:
            try:
                #睡眠
                if self.stop_update:
                    time.sleep(self.sleep_time)
                    continue
                #没有任务，休息
                job_str = self.predictjob_redis_conn.lpop(self.job_queue_name)
                if job_str is None:
                    time.sleep(self.sleep_time)
                    continue
                iter_number += 1
                #开始训练
                self.get_predict_job(job_str)
                if iter_number % 100 == 0:
                    iter_number = 1
                    self.calcu_auc()
                
            except:
                traceback.print_exc()
