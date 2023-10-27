package com.rev.messagepriorityqueue.service;

import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

import com.rev.messagepriorityqueue.model.Message;
import com.rev.messagepriorityqueue.provider.OrderedMessageQueueProvider;

public class OrderedMessageQueueService implements OrderedMessageQueueProvider {

	private static ExecutorService channelListenerPool;
	private Queue<Message> outQueue;
	private Queue<Message> messageQueueChannel1;
	private Queue<Message> messageQueueChannel2;
	private Queue<Message> messageQueueChannel3;
	private Queue<Message> messageQueueChannel4;
	private final ReentrantLock lock;
	
	
	
	public OrderedMessageQueueService() {
		this.outQueue = new LinkedBlockingQueue<Message>(5);
		this.messageQueueChannel1 = new LinkedBlockingQueue<Message>();
		this.messageQueueChannel2 = new LinkedBlockingQueue<Message>();
		this.messageQueueChannel3 = new LinkedBlockingQueue<Message>();
		this.messageQueueChannel4 = new LinkedBlockingQueue<Message>();
		channelListenerPool = Executors.newCachedThreadPool();
		this.lock = new ReentrantLock();
	}

	@Override
	public void queueMessage(Message m) {
		lock.lock();
		try {
			System.out.println("--------------------------------------------------------------------");
			System.out.println("NEW: " + m.toString());
			if(m.getSourceChannelId().equals("cn-01")) {
				messageQueueChannel1.add(m);
				printQueueItems("C1",messageQueueChannel1);
				printQueueItems("C2",messageQueueChannel2);
				printQueueItems("C3",messageQueueChannel3);
				printQueueItems("C4",messageQueueChannel4);
				
				consumeMessage(messageQueueChannel1,outQueue,1);
			} else if(m.getSourceChannelId().equals("cn-02")) {
				messageQueueChannel2.add(m);
				printQueueItems("C1",messageQueueChannel1);
				printQueueItems("C2",messageQueueChannel2);
				printQueueItems("C3",messageQueueChannel3);
				printQueueItems("C4",messageQueueChannel4);
				
				consumeMessage(messageQueueChannel2,outQueue,1);
			} else if(m.getSourceChannelId().equals("cn-03")) {
				messageQueueChannel3.add(m);
				printQueueItems("C1",messageQueueChannel1);
				printQueueItems("C2",messageQueueChannel2);
				printQueueItems("C3",messageQueueChannel3);
				printQueueItems("C4",messageQueueChannel4);
				
				consumeMessage(messageQueueChannel3,outQueue,3);
			} else {
				messageQueueChannel4.add(m);
				printQueueItems("C1",messageQueueChannel1);
				printQueueItems("C2",messageQueueChannel2);
				printQueueItems("C3",messageQueueChannel3);
				printQueueItems("C4",messageQueueChannel4);
				
				consumeMessage(messageQueueChannel4,outQueue,5);
			}
			System.out.println("--------------------------------------------------------------------");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			lock.unlock();
		}
	} 
	
	public static void printQueueItems(String channel, Queue<Message> messageQueue) {
		String str = "";
		for(Message m : messageQueue) {
			str = str.concat("[" + m.getMessageId() + "]");
		}
		
		System.out.println(channel + " :" + str);
	}
	
	public static void consumeMessage(Queue<Message> messageQueue, Queue<Message> outQueue, int transferSize) {
		 ChannelMessageConsumer consumer =
		          new ChannelMessageConsumer(messageQueue, outQueue, transferSize);
		 channelListenerPool.submit(consumer);
	}

	@Override
	public Queue<Message> getOutQueue() {
		return outQueue;
	}
	


}
