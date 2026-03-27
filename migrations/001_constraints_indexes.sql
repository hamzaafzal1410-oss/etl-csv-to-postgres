-- Constraints + indexes for the demo schema.
-- Safe to re-run (uses catalog checks).

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'customers_pk'
  ) THEN
    ALTER TABLE public.customers
      ADD CONSTRAINT customers_pk PRIMARY KEY (customer_id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'customers_email_uk'
  ) THEN
    ALTER TABLE public.customers
      ADD CONSTRAINT customers_email_uk UNIQUE (email);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'products_pk'
  ) THEN
    ALTER TABLE public.products
      ADD CONSTRAINT products_pk PRIMARY KEY (product_id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'products_sku_uk'
  ) THEN
    ALTER TABLE public.products
      ADD CONSTRAINT products_sku_uk UNIQUE (sku);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'orders_pk'
  ) THEN
    ALTER TABLE public.orders
      ADD CONSTRAINT orders_pk PRIMARY KEY (order_id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'orders_customer_fk'
  ) THEN
    ALTER TABLE public.orders
      ADD CONSTRAINT orders_customer_fk
      FOREIGN KEY (customer_id)
      REFERENCES public.customers(customer_id)
      ON UPDATE RESTRICT
      ON DELETE RESTRICT;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'order_items_pk'
  ) THEN
    ALTER TABLE public.order_items
      ADD CONSTRAINT order_items_pk PRIMARY KEY (order_id, line_number);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'order_items_order_fk'
  ) THEN
    ALTER TABLE public.order_items
      ADD CONSTRAINT order_items_order_fk
      FOREIGN KEY (order_id)
      REFERENCES public.orders(order_id)
      ON UPDATE RESTRICT
      ON DELETE RESTRICT;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'order_items_product_fk'
  ) THEN
    ALTER TABLE public.order_items
      ADD CONSTRAINT order_items_product_fk
      FOREIGN KEY (product_id)
      REFERENCES public.products(product_id)
      ON UPDATE RESTRICT
      ON DELETE RESTRICT;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON public.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON public.order_items(product_id);
