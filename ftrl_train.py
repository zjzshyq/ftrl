# -*- coding: utf-8 -*-
"""
Created on Thu Oct 26 10:50:40 2017

@author: ligong

@description:这是FTRL在线训练的主程序
"""
import json
import time
import math
import threading
import traceback
import random
import hashlib

def get_md5(string):
    """
    生成md5
    """
    return str(hashlib.new("md5",string).hexdigest())

class ftrl_train(threading.Thread):
    def __init__(self,thread_name,w_redis_conn,z_redis_conn,n_redis_conn,
                 trainjob_redis_conn,updater_threads,gd_threads):
        """
        初始化线程
        thread_name:线程名
        w_redis_conn:w参数的存放redis
        z_redis_conn:z参数的存放redis
        n_redis_conn:n参数的存放redis
        trainjob_redis_conn:训练任务的队列
        updater_threds:更新权重的线程
        gd_threads:更新梯度的线程
        """
        super(ftrl_train, self).__init__(name = thread_name)
        self.w_redis_conn = w_redis_conn
        self.z_redis_conn = z_redis_conn
        self.n_redis_conn = n_redis_conn
        self.trainjob_redis_conn = trainjob_redis_conn
        
        #各个线程
        self.updater_threads = updater_threads
        self.gd_threads = gd_threads
        
        self.setDaemon = True
        
        #任务队列
        self.job_queue_name = 'TRAIN_JOB_QUEUE'
        
        #是否停止更新
        self.stop_update = False
        
        #杀死
        self.dead = False
        
        #每次睡眠时间
        self.sleep_time = 10
        
        #记录每个训练的结果的字典，会动态更新
        self.train_result_set = 'TRAIN_RESULT_SET_%s'
        
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
    
    def kill(self):
        """
        杀死
        """
        self.dead = True
    
    def get_sigmoid_and_gt(self,val_name,wx,train_id,label):
        """
        计算sigmoid，不过会检查是否所有的数据都到达了，然后更新gt
        """
        key = self.train_result_set % train_id
        try:
            self.trainjob_redis_conn.hincrbyfloat(key,'wx',wx)
        except:
            print wx
        self.trainjob_redis_conn.hincrby(key,'ok',1)
        vals = int(self.trainjob_redis_conn.hget(key,'vals'))
        ok = int(self.trainjob_redis_conn.hget(key,'ok'))
        if vals > ok:
            #print '%s is not all ok!' % train_id
            return
        elif vals < ok:
            #print '%s ok nomber is bigger then vals, please check!' % train_id
            exit(-1)
        
        #print '%s is all ok!' % train_id   
        wx = float(self.trainjob_redis_conn.hget(key,'wx'))
        if wx > 0:
            t = math.exp(-wx)
            sigmoid = 1.0/(1.0+t)
        else:
            t = math.exp(wx)
            sigmoid = t/(1+t)
        print 'sigmoid is %s %s' % (sigmoid,label)
        data_dict = json.loads(self.trainjob_redis_conn.hget(key,'data'))
        alpha = float(self.trainjob_redis_conn.hget(key,'alpha'))
        
        #更新
        for (k,v) in data_dict.iteritems():
            if v != 0: 
                job_thread = self.get_thread(self.gd_threads)
                job = [k,alpha,sigmoid,label,v]
                job_thread.add_job(job)
                
        #清空数据
        self.trainjob_redis_conn.delete(key)
         
    def get_train_job(self,job_str):
        try:
            _id = get_md5(job_str)
            job = json.loads(job_str)
            data_dict = job['data']
            alpha = job['alpha']
            beta = job['beta']
            lambda_1 = job['lambda_1']
            lambda_2 = job['lambda_2']
            y = job['label']
            job_thread = self.get_thread(self.updater_threads)

            n = 0
            
            for (k,v) in data_dict.iteritems():
                if v != 0:
                    updater_job = [k,alpha,beta,lambda_1,lambda_2,self.get_sigmoid_and_gt,_id,y,v]
                    job_thread.add_job(updater_job)
                    n += 1
            key = self.train_result_set % _id
            #保存该条记录的数据
            d = {'vals':n,'wx':0.0,'ok':0,'data':json.dumps(data_dict),'alpha':alpha}
            self.trainjob_redis_conn.hmset(key,d)
            self.trainjob_redis_conn.expire(key,3600) 
            return True
        except:
            traceback.print_exc()
            return False

    def run(self):
        print '%s is running ...' % self.getName()
        while not self.dead:
            try:
                #睡眠
                if self.stop_update:
                    time.sleep(self.sleep_time)
                    continue
                #没有任务，休息
                job_str = self.trainjob_redis_conn.lpop(self.job_queue_name)
                if job_str is None:
                    time.sleep(self.sleep_time)
                    continue
                
                #开始训练
                self.get_train_job(job_str)
                
            except:
                traceback.print_exc()
        

