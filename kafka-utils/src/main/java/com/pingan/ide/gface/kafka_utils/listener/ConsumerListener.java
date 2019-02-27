package com.pingan.ide.gface.kafka_utils.listener;

import java.util.List;

import com.pingan.ide.gface.kafka_utils.vo.Msg;

public interface ConsumerListener {

	public void execute(List<Msg> messages);
}
