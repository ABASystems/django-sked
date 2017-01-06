class Coercion(object):
    def __call__(self, x):
        return x


class Operation(object):
    def __init__(self, coerce=None):
        self.coerce = coerce or Coercion()

    def __call__(self, x, y):
        return self.op(x, y)

    def final(self, x):
        return x


class Sum(Operation):
    def __call__(self, x, y):
        return x + y


class Max(Operation):
    def __call__(self, x, y):
        return y if y > x else x


class Average(Operation):
    def __init__(self):
        self.count = 1

    def __call__(self, x, y):
        self.count += 1
        return x + y

    def final(self, x):
        return x/self.count
