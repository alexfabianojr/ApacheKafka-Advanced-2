package br.com.alura.ecommerce;

import com.google.gson.*;

import java.lang.reflect.Type;

public class MessageAdapter implements JsonSerializer<Message>, JsonDeserializer<Message> {
    @Override
    public JsonElement serialize(Message message, Type type, JsonSerializationContext jsonSerializationContext) {
        var object = new JsonObject();
        object.add("payload", jsonSerializationContext.serialize(message.getPayload()));
        object.add("id", jsonSerializationContext.serialize(message.getId()));
        object.addProperty("type", message.getPayload().getClass().getName());
        return object;
    }

    @Override
    public Message deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        var obj = jsonElement.getAsJsonObject();
        var payloadType = obj.get("type").getAsString();
        var correlationId = (CorrelationId) jsonDeserializationContext.deserialize(obj.get("correlationId"), CorrelationId.class);
        try {
            var payload = jsonDeserializationContext
                    .deserialize(obj.get("payload"), Class.forName(payloadType));
        return new Message(correlationId, payload);
        } catch (ClassNotFoundException e) {
            throw new JsonParseException(e);
        }
    }
}
