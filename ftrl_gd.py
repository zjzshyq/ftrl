# -*- coding: utf-8 -*-
"""
Created on Thu Oct 26 10:28:17 2017

@author: ligong

@description:这是FTRL中计算梯度的程序
"""
import Queue
import time
import math
import threading
import traceback

class ftrl_gd(threading.Thread):
    def __init__(self,thread_name,w_redis_conn,z_redis_conn,n_redis_conn):
        """
        初始化线程
        thread_name:线程名
        w_redis_conn:w参数的存放redis
        z_redis_conn:z参数的存放redis
        n_redis_conn:n参数的存放redis
        """
        super(ftrl_gd, self).__init__(name = thread_name)
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
        
    def get_w_value(self,val_name):
        """
        获得w的值
        """
        wscore = self.w_redis_conn.get(val_name)
        if wscore is None:
            return 0
        else:
            return float(wscore)
    
    def get_n_value(self,val_name):
        """
        获得n的值
        """
        nscore = self.n_redis_conn.get(val_name)
        if nscore is None:
            return 0
        else:
            return float(nscore)
        
    def update_z(self,val_name,value):
        """
        更新z的值
        """
        self.z_redis_conn.incrbyfloat(val_name,value)
    
    def update_n(self,val_name,value):
        """
        更新n的值
        """
        self.n_redis_conn.incrbyfloat(val_name,value)
        
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
                val_name,alpha,pscore,score,x = job[0],job[1],job[2],job[3],job[4]
                
                g = (pscore-score)*x
                n = self.get_n_value(val_name)
                tmp = n+g*g
                delta = ((math.sqrt(tmp) - math.sqrt(n)))/alpha
                w = self.get_w_value(val_name)
                self.update_z(val_name,g-delta*w)
                self.update_n(val_name,g*g)
                '''
                if val_name == 'first_level_std_detail_len':
                    print tmp,g-delta*w,pscore,score
                '''
            except:
                traceback.print_exc()

