package com.mattstine.dddworkshop.pizzashop.kitchen;

import com.mattstine.dddworkshop.pizzashop.infrastructure.events.ports.EventLog;
import com.mattstine.dddworkshop.pizzashop.infrastructure.events.ports.Topic;
import com.mattstine.dddworkshop.pizzashop.infrastructure.repository.adapters.InProcessEventSourcedRepository;
import com.mattstine.dddworkshop.pizzashop.ordering.OnlineOrderRef;

import java.util.HashMap;
import java.util.Map;

final class InProcessEventSourcedKitchenOrderRepository extends InProcessEventSourcedRepository<KitchenOrderRef, KitchenOrder, KitchenOrder.OrderState, KitchenOrderEvent, KitchenOrderAddedEvent> implements KitchenOrderRepository {
    private final Map<OnlineOrderRef, KitchenOrderRef> onlineOrderRefToKitchenOrderRef;

    InProcessEventSourcedKitchenOrderRepository(EventLog eventLog, Topic topic) {
        super(eventLog,
                KitchenOrderRef.class,
                KitchenOrder.class,
                KitchenOrder.OrderState.class,
                KitchenOrderAddedEvent.class,
                topic);

        // create an index - OnlineOrderRef idx, for findByOnlineOrderRef() 
        // map<OnlineOrderRef, KitchenOrderRef> ; value is refereced instance of KitchenOrder  
        onlineOrderRefToKitchenOrderRef = new HashMap<>();   

        // BUILD ALTERNATE INDEX on new KitchenOrderAddedEvent
        // subscribe to KitchenOrder topic; whenever event Repository.KitchenOrderAdd 
        // is spawned, this event stores state of the Aggragate, and ref to Aggregate,
        // while map<OnlineOrderRef, KitchenOrderRef>
        eventLog.subscribe(topic, (e) -> {
            if (e instanceof KitchenOrderAddedEvent) {
                onlineOrderRefToKitchenOrderRef.put(((KitchenOrderAddedEvent) e)
                                .getState()
                                .getOnlineOrderRef(),                  // Key
                        ((KitchenOrderAddedEvent) e).getRef());        // value put
            }
        });
    }

    @Override
    // Design findByOnlineOrderRef(): use above created OnlineOrderRef index, to find a KitchenOrder by its OnlineOrderRef. 
    // Whenver KitchenOrder is addded to the Repository, update OnlineOrderRef index.
    //Result: InProcessEventSourceKitchenOrderRepository.findByOnlineOrderRef()
    public KitchenOrder findByOnlineOrderRef(OnlineOrderRef onlineOrderRef) {
        // obtain KitchenOrderRef as value in the map<OnlineOrderRef, KithenOrderRef>
        KitchenOrderRef kitchenOrderRef = onlineOrderRefToKitchenOrderRef.get(onlineOrderRef);
        return findByRef(kitchenOrderRef);
    }
}
