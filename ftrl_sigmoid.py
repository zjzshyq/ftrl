# -*- coding: utf-8 -*-
"""
Created on Thu Oct 26 09:57:16 2017

@author: ligong

@description:这是FTRL中计算sigmoid值得程序
"""
import Queue
import time
import math
import threading
import traceback

def calcu_sigmoid(value_dict,redis_conn):
    """
    计算值
    """
    keys,values = value_dict.keys(),value_dict.values()
    ws = redis_conn.mget(keys)
    r = 0.0
    for (v,w) in zip(values,ws):
        if w is not None:
            r += float(w)*v
    if r > 0:
        t = math.exp(-r)
        #print 'ddddd %s %s %s\n\n\n\n' % (r,t,1.0/(1.0+t))
        return 1.0/(1.0+t)
    t = math.exp(r)
    #print 'eeeeeeeeeeeeeeee %s %s %s\n\n\n\n' % (r,t,t/(1.0+t))
    return t/(1.0+t)

class ftrl_sigmoid(threading.Thread):
    def __init__(self,thread_name,w_redis_conn):
        """
        初始化线程
        thread_name:线程名
        w_redis_conn:w参数的存放redis
        """
        super(ftrl_sigmoid, self).__init__(name = thread_name)
        self.w_redis_conn = w_redis_conn
        
        self.setDaemon = True
        
        #任务队列
        self.job_queue = Queue.Queue()
        
        #是否停止更新
        self.stop_update = False
        
        #杀死
        self.dead = False
        
        #每次睡眠时间
        self.sleep_time = 10
        
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
    
    def kill(self):
        """
        杀死
        """
        self.dead = True
    
    def get_z_value(self,val_name):
        """
        获得z的值
        """
        zscore = self.z_redis_conn.get(val_name)
        if zscore is None:
            return 0
        else:
            return float(zscore)
        
    def add_job(self,job):
        """
        添加任务
        """
        self.job_queue.put(job)
        
    def run(self):
        print '%s is running ...' % self.getName()
        while not self.dead:
            try:
                #睡眠
                if self.stop_update:
                    time.sleep(self.sleep_time)
                    continue
                #没有任务，休息
                if self.job_queue.empty():
                    time.sleep(self.sleep_time)
                    continue
                job = self.job_queue.get_nowait()
                value_dict,call_back,y = job[0],job[1],job[2]
                predict_score= calcu_sigmoid(value_dict,self.w_redis_conn)
                #回调
                if call_back is not None:
                    call_back(y,predict_score)
            except:
                traceback.print_exc()
