package part1recap

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaRecap extends App {

  // values and variables
  val aBoolean: Boolean = false

  // expressions
  val anIfExpression = if(2 > 3) "bigger" else "smaller"

  // instructions vs expressions
  // instructions are fundamental building blocks of imperative programs like C++ or Python or Java
  // instructions are executed 1-by-1 and a program is just a sequence of instructions
  // instruction-like expressions are denoted in scala by the type Unit
  // expressions are the building blocks of functional programs like scala
  // expressions are evaluated, meaning that they can be reduced to a single value
  val theUnit = println("Hello, Scala") // Unit = "no meaningful value" = void in other languages

  // functions
  def myFunction(x: Int) = 42

  // OOP
  // object orientation in scala is done with single class inheritance
  // scala also has interfaces/traits; we can define unimplemented methods
  class Animal
  class Cat extends Animal
  trait Carnivore {
    def eat(animal: Animal): Unit
  }

  class Crocodile extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = println("Crunch!")
  }

  // singleton pattern
  object MySingleton

  // companions
  // compansion object/class can see eachother's private members and other nice features that scala gives us
  object Carnivore

  // generics
  // has advanced features like covariance
  trait MyList[A]

  // method notation
  val x = 1 + 2
  val y = 1.+(2)

  // Functional Programming
  // Function1[Int,Int] is sugared with Int => Int (anoynmous functions/lambda)
  val incrementerOld: Function1[Int, Int] = new Function1[Int, Int] {
    override def apply(i: Int): Int = x + 1
  }
  val incrementer: Int => Int = x => x + 1
  val incremented = incrementer(42)

  // map, flatMap, filter (higher order function)
  val processedList = List(1,2,3).map(incrementer)

  // Pattern Matching
  val unknown: Any = 45
  val ordinal = unknown match {
    case 1 => "first"
    case 2 => "second"
    case _ => "unknown"
  }

  // try-catch; still done with pattern matching
  try {
    throw new NullPointerException
  } catch {
    case _: NullPointerException => "some returned value"
    case _: Throwable => "something else"
  }

  // Future
  // they abstract away computations on seperate threads
  import scala.concurrent.ExecutionContext.Implicits.global // implicit value used as a platform for running threads
  val aFuture = Future {
    // some expensive computation, runs on another thread
    42
  }

  aFuture.onComplete {
    case Success(meaningOfLife) => println(s"I've found $meaningOfLife")
    case Failure(ex) => println(s"I have failed: $ex")
  }

  // Partial functions
  val aPartialFunctionOne = (x: Int) => x match {
    case 1 => 43
    case 8 => 56
    case _ => 999
  }
  val aPartialFunction: PartialFunction[Int, Int] = {
    case 1 => 43
    case 8 => 56
    case _ => 999
  }

  // Implicits

  // auto-injection by the compiler (implicits are automatically injected by the compiler)
  def methodWithImplicitArgument(implicit x: Int) = x + 43
  implicit val implicitInt = 67
  val implicitCall = methodWithImplicitArgument

  // implicit conversions - implicit defs
  case class Person(name: String) {
    def greet = println(s"Hi, my name is $name")
  }

  implicit def fromStringToPerson(name: String) = Person(name)
  "Bob".greet // fromStringToPerson("Bob").greet; compiler sees that greet is not a method of string, so it searches

  // implicit conversion - implicit classes
  implicit class Dog(name: String) {
    def bark = println("Bark!")
  }
  "Lassie".bark

  /*
    - local scope (explicitely defining implicit value in local scope)
    - imported scope (e.g. any scope you imported, like when you import the global for future)
    - companion objects of the types involved in the method call
      - e.g. List(1,2,3).sorted
      - takes an implicit Ordering param
      - compiler will look for it last in the companion object of List or in the companion object Int
      - Int is involved in the method call; for Int we already have implicit ordering
   */


}
