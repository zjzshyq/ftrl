# -*- coding: utf-8 -*-
"""
Created on Thu Oct 26 09:17:14 2017

@author: ligong

@description:这是FTRL中计算每个特征的权重的程序
"""
import Queue
import time
import math
import threading
import traceback

class ftrl_weight_updater(threading.Thread):
    def __init__(self,thread_name,w_redis_conn,z_redis_conn,n_redis_conn):
        """
        初始化线程
        thread_name:线程名
        w_redis_conn:w参数的存放redis
        z_redis_conn:z参数的存放redis
        n_redis_conn:n参数的存放redis
        """
        super(ftrl_weight_updater, self).__init__(name = thread_name)
        self.w_redis_conn = w_redis_conn
        self.z_redis_conn = z_redis_conn
        self.n_redis_conn = n_redis_conn
        
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
    
    def get_n_value(self,val_name):
        """
        获得n的值
        """
        nscore = self.n_redis_conn.get(val_name)
        if nscore is None:
            return 0
        else:
            return float(nscore)
        
    def update_w(self,val_name,w):
        """
        更新权重值
        """
        self.w_redis_conn.set(val_name,w)
      
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
                val_name,alpha,beta,lambda_1,lambda_2,call_back,train_id,label,x = job[0],job[1],job[2],job[3],job[4],job[5],job[6],job[7],job[8]
                
                #alpha,beta,lambda_1,lambda_2 = float(alpha),float(beta),float(lambda_1),float(lambda_2)
                
                z = self.get_z_value(val_name)
                w = 0
                
                if abs(z) > lambda_1:
                    n = self.get_n_value(val_name)
                    tmp = -1 if z  < 0 else 1
                    w = -(z - tmp*lambda_1)/((beta+math.sqrt(n))/alpha+lambda_2)
                    '''
                    if val_name == 'first_level_std_detail_len':
                        print 'first_level_std_detail_len:%s' % w   
                    '''
                self.update_w(val_name,w)
                #回调
                if call_back is not None:
                    call_back(val_name,x*w,train_id,label)
                
            except:
                traceback.print_exc()
        
