package co.ogury.segmentation

case class Transaction(
  id: String,
  customerId: String,
  productId: String,
  date: String,
  price: Int,
  quantity: Int
)
