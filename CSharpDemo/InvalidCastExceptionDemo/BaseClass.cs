/// <summary>
/// Recurrent the exception:
/// InvalidCastException: Unable To Cast Objects of type [base] to type [subclass]
/// 
/// Doc link: https://stackoverflow.com/questions/5240143/invalidcastexception-unable-to-cast-objects-of-type-base-to-type-subclass
/// You've got it in reverse: A cast from an object of a base class to a subclass will always fail, because the base class has only the properties of the the base class (not the subclass).
/// Since, as you say, the subclass has all the properties of the base class (it "is-a" base -class object), then a cast from the subclass to the base class will always succeed, but never the reverse.
/// In other words, you can think of all leopards as cats, but you cannot take an arbitrary cat and treat it like a leopard (unless it's already a leopard to begin with).
/// </summary>
namespace CSharpDemo.InvalidCastExceptionDemo
{
    class BaseClass
    {
        public string Name { get; set; }
    }
}
