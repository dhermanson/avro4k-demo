package org.example

import kotlinx.serialization.Serializable


@Serializable
data class Ingredient(val name: String, val sugar: Double)

@Serializable
data class Pizza(val name: String, val ingredients: List<Ingredient>, val topping: Ingredient?, val vegetarian: Boolean)




@Serializable
sealed interface AccountEvent {

    @Serializable
    data class Opened(val initialDeposit: ULong) : AccountEvent
    @Serializable
    data class Credited(val amount: ULong) : AccountEvent
    @Serializable
    data class Debited(val amount: ULong) : AccountEvent

}