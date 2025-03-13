from locust import HttpUser, task, between
import random


class VegetableSalesUser(HttpUser):
    # Wait time between 0.01 and 0.1 seconds to achieve high load
    wait_time = between(0.01, 0.1)

    @task(50)  # 50% of requests will be POST
    def post_data(self):
        """Simulate posting sales data"""
        # Generate random valid data
        year = random.randint(2020, 2023)
        week = random.randint(1, 52)
        date = f"{year}-{week:02d}"

        vegetables = [
            "tomato",
            "carrot",
            "potato",
            "onion",
            "pepper",
            "tomate",
            "carotte",
            "pomme de terre",
            "oignon",
            "poivron",
        ]
        vegetable = random.choice(vegetables)

        kilo_sold = random.randint(50, 1000)

        self.client.post(
            "/post_sales/",
            json=[{"date": date, "vegetable": vegetable, "kilo_sold": kilo_sold}],
        )

    @task(25)  # 25% of requests will be GET raw
    def get_raw_sales(self):
        """Simulate getting raw sales data"""
        self.client.get("/get_raw_sales/")

    @task(25)  # 25% of requests will be GET monthly
    def get_monthly_sales(self):
        """Simulate getting monthly sales data"""
        # Randomly choose whether to filter outliers
        remove_outliers = random.choice([True, False])
        self.client.get(
            f"/get_monthly_sales/?remove_outliers={str(remove_outliers).lower()}"
        )


# To run:
# locust -f locustfile.py --host=http://localhost:8000
