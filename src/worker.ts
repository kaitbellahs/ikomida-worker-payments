import { createRequire } from 'module';
import { Domain, Utils, DBModels, Helpers, Types } from '@ikomida/shared-backend';
import { Channel, Message } from 'amqplib';
const require = createRequire(import.meta.url);
let { name } = require('../package.json');
name = name
  .replace(/^(@\S+\/)?(svelte-)?(\S+)/, '$3')
  .replace(/^\w/, (m: string) => m.toUpperCase())
  .replace(/-\w/g, (m: string[]) => m[1].toUpperCase());

class PaymentsWorker {
  amqp;
  logger;

  constructor() {
    this.logger = Utils.Logger.getInstance(name);
    this.amqp = new Domain.RabbitMQ(this.logger);
  }

  async run() {
    try {
      await this.amqp.listenToMessages(Domain.RabbitMQ.PAYMENT_QUEUE, this.processMessages.bind(this));
    } catch (error: any) {
      this.logger.error(error);
    }
  }

  async processMessages(message: Message, channel: Channel) {
    try {
      this.logger.log(` [x] ${message.fields.routingKey}: message received: '${message.content.toString('utf8')}'`);
      const payload: Types.Classes.CAMQPPayload<string> = Types.Classes.CAMQPPayload.fromObject(JSON.parse(message.content?.toString('utf8') ?? '{}'));
      if (payload.method === 'cancelPayment') {
        const paymentID = String(payload?.object);
        const models = await this.getModel(paymentID);
        if (!models) {
          this.logger.error('Nao foi possivel obter o pagamento do banco de dados');
          channel.ack(message);
          return false;
        }
        const { paymentGateway, userPaymentModel } = models;
        let n = 0;
        const startTime = new Date().getTime();
        let i = 0;
        for (i = 1; i <= 5; i++) {
          const cancelChargeObject = Types.Classes.Pagseguro.CPagSeguroCreateCharge.init('', userPaymentModel?.amount ?? 0, Types.Types.Pagseguro.TPagSeguroPaymentMethod.CREDIT_CARD, '', undefined, undefined, undefined, undefined, undefined, undefined, userPaymentModel?.gatewayPaymentID);
          const chargeResult = await paymentGateway?.cancelCharge(cancelChargeObject);
          if (chargeResult) {
            userPaymentModel.status = chargeResult?.status;
            await userPaymentModel.save();
            this.logger.log('Cobranca foi cancelada com sucesso');
            channel.ack(message);
            return true;
          }
          n += i;
          await Utils.System.sleep(n * 4000);
        }
        this.logger.error(`nao foi possivel cancelar cobranca após ${i} tentativas em ${(startTime - new Date().getTime()) / 1000}s.`);
      }
    } catch (error: any) {
      this.logger.error(`nao foi possivel cancelar cobranca, erro inesperado:`);
      this.logger.error(error);
    }
  }

  async getModel(id: string) {
    try {
      const userPaymentModel = await DBModels.UserPaymentModel.findOne({
        where: {
          id,
        },
        include: {
          model: DBModels.ContractModel,
          required: true,
          include: [
            {
              model: DBModels.VendorSettingsModel,
              required: false,
              include: [
                {
                  model: DBModels.VendorPaymentGatewayModel,
                  required: false,
                },
              ],
            },
          ],
        },
      });
      if (!userPaymentModel?.contract) {
        this.logger.error('Contrato nao foi localizado');
        return false;
      }
      const pagseguroHelper = new Helpers.PagseguroHelper(this.logger);
      const paymentGateway = await pagseguroHelper.configure(
        userPaymentModel?.contract?.vendorSettings?.vendorPaymentGateway,
      );
      if (!paymentGateway) {
        this.logger.error('Nao foi possivel obter gateway');
        return false;
      }
      return { paymentGateway, userPaymentModel };
    } catch (exception) {
      this.logger.error(exception);
      return false;
    }
  }
}

await new PaymentsWorker().run();
