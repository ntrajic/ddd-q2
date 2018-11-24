package com.mattstine.dddworkshop.pizzashop.kitchen;

import com.mattstine.dddworkshop.pizzashop.infrastructure.events.ports.EventLog;
import com.mattstine.dddworkshop.pizzashop.infrastructure.events.ports.Topic;
import com.mattstine.dddworkshop.pizzashop.infrastructure.repository.adapters.InProcessEventSourcedRepository;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

final class InProcessEventSourcedPizzaRepository extends InProcessEventSourcedRepository<PizzaRef, Pizza, Pizza.PizzaState, PizzaEvent, PizzaAddedEvent> implements PizzaRepository {
    private final Map<KitchenOrderRef, Set<PizzaRef>> kitchenOrderRefToPizzaRefSet;

    InProcessEventSourcedPizzaRepository(EventLog eventLog, Topic pizzas) {
        super(eventLog, PizzaRef.class, Pizza.class, Pizza.PizzaState.class, PizzaAddedEvent.class, pizzas);

        // create map<KitchenOrderRef, Set<Pizza>>
        kitchenOrderRefToPizzaRefSet = new HashMap<>();

        // dual index, index is KitchenOrderRef, we want all Pizzas in his KithenOrder
        // all referenes are rehydrate from the event object PizzaAddedEvent
        // subscibe to pizza topic event:
        eventLog.subscribe(new Topic("pizzas"), e -> {
            if (e instanceof PizzaAddedEvent) {
                PizzaAddedEvent pae = (PizzaAddedEvent) e;
                // PERVERT - init map with KitchenOrderRef and set<PIzza>, and if no sets are  yet created, create an empty map 
                Set<PizzaRef> pizzaRefs = kitchenOrderRefToPizzaRefSet.computeIfAbsent(pae.getState().getKitchenOrderRef(), k -> new HashSet<>());
                pizzaRefs.add(pae.getRef());   // append the pizz from event to the Set of Pizzas givent by Ref
            }
        });
    }

    @Override
    // PERVERT: set<Pizza> (found by findPizzasByKitchenOrderRef(KOR))
    // convert to stream of PIzzas, then map (i.e.e apply findByRef() function on each Pizza in the Set)
    // to get stream of Pizza referenes for the KithenOrderRef, which are converted into a Set<Pizza>
    public Set<Pizza> findPizzasByKitchenOrderRef(KitchenOrderRef kitchenOrderRef) {
        return kitchenOrderRefToPizzaRefSet.get(kitchenOrderRef).stream()
                .map(this::findByRef)
                .collect(Collectors.toSet());
    }
}
